package com.chaoppo.db.storm.spout;

import com.chaoppo.db.storm.connectivity.DiscoveryClient;
import com.chaoppo.db.storm.error.StormAdapterException;
import com.chaoppo.db.storm.error.StormAdapterRuntimeException;
import com.chaoppo.db.storm.eventhub.BulkEventData;
import com.chaoppo.db.storm.eventhub.BulkPartitionManager;
import com.chaoppo.db.storm.eventhub.EventHubDiscoveryResponse;
import com.chaoppo.db.storm.eventhub.IBulkPartitionManager;
import com.chaoppo.db.storm.util.Constants;
import com.microsoft.azure.eventhubs.EventData;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.storm.Config;
import org.apache.storm.shade.org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

public class BulkEventHubSpout extends BaseRichSpout {

    private static final long serialVersionUID = -4648913950714538862L;
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkEventHubSpout.class);

    /* Configuration for partition management and bulk read from Event Hubs */
    private EventHubSpoutConfig eventHubConfig;
    // EventDataScheme which contains message and meta data as well
    private IEventDataScheme scheme;
    private int checkpointIntervalInSeconds;
    /* Storing offset state , will be zookeeper */
    private IStateStore stateStore;
    private StaticPartitionCoordinator partitionCoordinator;
    private IPartitionManagerFactory pmFactory;
    private IEventHubReceiverFactory recvFactory;
    private int totalTasks;
    private int taskIndex;
    /* Time at which the checkpoint was actually done. */
    private long lastCheckpointTime;
    private int currentPartitionIndex = -1;

    /* Variables to retain across activation / deactivation calls */
    @SuppressWarnings("rawtypes")
    private Map spoutConfig;
    private TopologyContext spoutContext;
    private SpoutOutputCollector spoutOutputCollector;
    private Long connectionExpiryTs;
    private DiscoveryClient discoveryClient;
    private EventHubDiscoveryResponse eventHubDiscoveryResponse;

    private String eventHubSubscriptionKey;
    private String pdsUrlForTelemetryData;
    private String httpProxyHost;
    private Integer httpProxyPort;

    public BulkEventHubSpout(Integer bulkMessageCount) {
        this(null, null, null, null, bulkMessageCount);
    }

    public BulkEventHubSpout(IStateStore store, IPartitionManagerFactory pmFactory,
            IEventHubReceiverFactory recvFactory, IEventDataScheme scheme, Integer bulkMessageCount) {

        this.scheme = scheme;
        this.stateStore = store;
        this.pmFactory = pmFactory;
        this.recvFactory = recvFactory;

        if (this.scheme == null) {
            this.scheme = new EventDataScheme();
        }

        if (bulkMessageCount == null || bulkMessageCount <= 0) {
            bulkMessageCount = 20;
        }

        final int cnt = bulkMessageCount.intValue();
        if (pmFactory == null) {
            this.pmFactory = (IPartitionManagerFactory) (sc, partitionId, istateStore, receiver) -> new BulkPartitionManager(
                    sc, partitionId, istateStore, receiver, cnt);
        }
        if (this.recvFactory == null) {
            this.recvFactory = new IEventHubReceiverFactory() {

                private static final long serialVersionUID = -7761866292285649100L;

                @Override
                public IEventHubReceiver create(EventHubSpoutConfig spoutConfig, String partitionId) {
                    return new EventHubReceiverImpl(spoutConfig, partitionId);
                }
            };
        }

    }

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
        LOGGER.info("begin: open()");
        this.spoutConfig = config;
        this.spoutContext = context;
        this.spoutOutputCollector = collector;
        this.createEventHub();
        this.prepareEventHubListen();
        LOGGER.info("end open()");
    }

    private void createEventHub() {

        String topologyName = (String) this.spoutConfig.get(Config.TOPOLOGY_NAME);

        initializeDiscoveryClient();

        this.eventHubConfig = DiscoveryClient.generateEventHubSpoutConfig(this.eventHubDiscoveryResponse);
        this.eventHubConfig.withEventDataScheme(this.scheme);
        this.eventHubConfig.setTopologyName(topologyName);
        this.checkpointIntervalInSeconds = this.eventHubConfig.getCheckpointIntervalInSeconds();
        this.lastCheckpointTime = System.currentTimeMillis();
    }

    private DiscoveryClient initializeDiscoveryClient() {
        if (this.discoveryClient == null) {
            LOGGER.info("discoveryClient is null; initializing...");
            this.discoveryClient = DiscoveryClient
                    .getInstance(this.eventHubSubscriptionKey, this.pdsUrlForTelemetryData, this.httpProxyHost,
                            this.httpProxyPort);
            LOGGER.info("discoveryClient initialization completed!");
        }
        return this.discoveryClient;
    }

    private void prepareEventHubListen() {
        LOGGER.info("begin: prepareEventHubListen()");
        String topologyName = (String) this.spoutConfig.get(Config.TOPOLOGY_NAME);
        eventHubConfig.setTopologyName(topologyName);

        this.totalTasks = this.spoutContext.getComponentTasks(this.spoutContext.getThisComponentId()).size();
        this.taskIndex = this.spoutContext.getThisTaskIndex();
        if (totalTasks > eventHubConfig.getPartitionCount()) {
            throw new StormAdapterRuntimeException("total tasks of EventHubSpout is greater than partition count.");
        }

        LOGGER.info(
                String.format("topologyName: %s, totalTasks: %d, taskIndex: %d", topologyName, totalTasks, taskIndex));

        try {
            preparePartitions(this.spoutConfig, totalTasks, taskIndex, this.spoutOutputCollector);
        } catch (Exception e) {
            this.spoutOutputCollector.reportError(e);
            throw new StormAdapterRuntimeException(e);
        }
        LOGGER.info("end: prepareEventHubListen()");
    }

    @SuppressWarnings("rawtypes")
    public void preparePartitions(Map config, int totalTasks, int taskIndex,
            SpoutOutputCollector collector) throws Exception {

        if (stateStore == null) {
            String zkEndpointAddress = eventHubConfig.getZkConnectionString();
            LOGGER.info("zkEndpointAddress  [" + zkEndpointAddress + "]");
            if (zkEndpointAddress == null || zkEndpointAddress.length() == 0) {
                // use storm's zookeeper servers if not specified.
                @SuppressWarnings("unchecked") List<String> zkServers = (List<String>) config
                        .get(Config.STORM_ZOOKEEPER_SERVERS);
                Integer zkPort = ((Number) config.get(Config.STORM_ZOOKEEPER_PORT)).intValue();
                StringBuilder sb = new StringBuilder();
                for (String zk : zkServers) {
                    if (sb.length() > 0) {
                        sb.append(',');
                    }
                    sb.append(zk + ":" + zkPort);
                }
                zkEndpointAddress = sb.toString();
            }
            stateStore = new ZookeeperStateStore(zkEndpointAddress,
                    Integer.parseInt(config.get(Config.STORM_ZOOKEEPER_RETRY_TIMES).toString()),
                    Integer.parseInt(config.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL).toString()));
        }
        try {
            stateStore.open();
        } catch (IllegalStateException e) {
            LOGGER.info("CuratorFrameworkImpl is already started. Cannot be started more than once");
        }

        partitionCoordinator = new StaticPartitionCoordinator(eventHubConfig, taskIndex, totalTasks, stateStore,
                pmFactory, recvFactory);

        for (IPartitionManager partitionManager : partitionCoordinator.getMyPartitionManagers()) {
            partitionManager.open();
        }
    }

    @Override
    public void nextTuple() {
        long ts = Instant.now().atOffset(ZoneOffset.UTC).toEpochSecond();
        long threadId = Thread.currentThread().getId();

        if (Math.subtractExact(ts, connectionExpiryTs) > -1) {
            LOGGER.info(threadId + "-" + ts + " Token needs to renewed, connectionExpiryTs = " + connectionExpiryTs);
            try {
                resetEventHubSpout();
            } catch (Exception e) {
                LOGGER.info(
                        threadId + "-" + ts + " nextTuple unable to reset event hub on token expiry " + ExceptionUtils
                                .getFullStackTrace(e));
                return;
            }
            return;
        }

        BulkEventData eventData = null;
        List<IPartitionManager> partitionManagers = partitionCoordinator.getMyPartitionManagers();
        for (int i = 0; i < partitionManagers.size(); i++) {
            currentPartitionIndex = (currentPartitionIndex + 1) % partitionManagers.size();
            IBulkPartitionManager partitionManager = (IBulkPartitionManager) partitionManagers
                    .get(currentPartitionIndex);

            if (partitionManager == null) {
                throw new StormAdapterRuntimeException("partitionManager doesn't exist.");
            }
            eventData = partitionManager.receiveBulk();
            if (eventData != null) {
                break;
            }
        }

        if (eventData != null) {
            List<EventData> messages = eventData.getMessages();

            this.spoutOutputCollector.emit(new Values(messages));
        }
        checkpointIfNeeded();
        // We don't need to sleep here because the IPartitionManager.receive() is
        // a blocked call so it's fine to call this function in a tight loop.
    }

    private synchronized void resetEventHubSpout() throws StormAdapterException {
        long id = Thread.currentThread().getId();
        long startTs = Instant.now().atOffset(ZoneOffset.UTC).toEpochSecond();
        LOGGER.info(id + "-" + startTs + " resetEventHubSpout: begin " + Thread.currentThread().getId() + ", startTs = "
                + startTs);
        try {
            LOGGER.info(id + "-" + startTs + " resetEventHubSpout: start ");

            checkpoint();

            initializeDiscoveryClient();
            LOGGER.info(id + "-" + startTs + " name=" + this.eventHubDiscoveryResponse.getName() + ", region = "
                    + this.eventHubDiscoveryResponse.getRegion());
            this.eventHubDiscoveryResponse = this.discoveryClient
                    .getEventHubDiscoveryResponse(this.eventHubDiscoveryResponse, this.eventHubConfig.getNamespace());

            if (this.eventHubDiscoveryResponse == null) {
                LOGGER.error(id + "-" + startTs + " unable to get EventHubDiscoveryResponse using original response [ "
                        + this.eventHubDiscoveryResponse + " ]");
                return;
            }

            EventHubSpoutConfig renewedSpoutConfig = DiscoveryClient
                    .generateEventHubSpoutConfig(this.eventHubDiscoveryResponse);
            renewedSpoutConfig.withEventDataScheme(this.scheme);

            BeanUtils.copyProperties(this.eventHubConfig, renewedSpoutConfig);

            String topologyName = (String) this.spoutConfig.get(Config.TOPOLOGY_NAME);
            this.eventHubConfig.setTopologyName(topologyName);
            LOGGER.info(id + "-" + startTs + " received eventhub config from pds");

            closePartitionCoordinator();
            LOGGER.info(id + "-" + startTs + " closePartitionCoordinator done");

            partitionCoordinator = new StaticPartitionCoordinator(eventHubConfig, taskIndex, totalTasks, stateStore,
                    this.pmFactory, this.recvFactory);

            // lambda should not be used as we want open to throw exception and we need to
            // catch it for
            // retry.
            for (IPartitionManager pm : partitionCoordinator.getMyPartitionManagers()) {
                pm.open();
            }

            connectionExpiryTs = eventHubDiscoveryResponse.getConnectionExpiry();
            LOGGER.info(id + "-" + startTs + " new connection expiry ts set to = " + connectionExpiryTs);
        } catch (Exception e) {
            LOGGER.info(id + "-" + startTs + " resetEventHubSpout, Exception occurred::" + ExceptionUtils
                    .getFullStackTrace(e));
        }

        long endTs = Instant.now().atOffset(ZoneOffset.UTC).toEpochSecond();
        LOGGER.info(id + "-" + startTs + " resetEventHubSpout: end::: TimeTaken = " + (endTs - startTs));
    }

    private void closePartitionCoordinator() {
        if (partitionCoordinator == null) {
            return;
        }
        try {
            partitionCoordinator.getMyPartitionManagers().forEach(pm -> {
                if (pm != null) {
                    pm.close();
                }
            });
        } catch (Exception e) {
            LOGGER.info("closePartitionCoordinator: " + ExceptionUtils.getFullStackTrace(e));
        }
    }

    private void checkpoint() {
        if (partitionCoordinator != null) {
            for (IPartitionManager partitionManager : partitionCoordinator.getMyPartitionManagers()) {
                if (partitionManager != null) {
                    partitionManager.checkpoint();
                }
            }
        }
    }

    private void checkpointIfNeeded() {
        long nextCheckpointTime = lastCheckpointTime + checkpointIntervalInSeconds * 1000;
        if (nextCheckpointTime < System.currentTimeMillis()) {
            checkpoint();
            lastCheckpointTime = System.currentTimeMillis();
        }
    }

    @Override
    public void close() {
        LOGGER.info("begin: close()");
        if (partitionCoordinator != null) {
            for (IPartitionManager partitionManager : partitionCoordinator.getMyPartitionManagers()) {
                if (partitionManager != null) {
                    partitionManager.close();
                }
            }
        }
        if (stateStore != null) {
            stateStore.close();
        }
        super.close();
        LOGGER.info("end: close()");
    }

    @Override
    public void activate() {
        LOGGER.info("begin: activate()");
        super.activate();
        LOGGER.info("end activate()");
    }

    @Override
    public void ack(Object msgId) {
        MessageId messageId = (MessageId) msgId;
        IPartitionManager partitionManager = partitionCoordinator.getPartitionManager(messageId.getPartitionId());
        String offset = messageId.getOffset();
        partitionManager.ack(offset);
    }

    @Override
    public void fail(Object msgId) {
        MessageId messageId = (MessageId) msgId;
        IPartitionManager partitionManager = partitionCoordinator.getPartitionManager(messageId.getPartitionId());
        String offset = messageId.getOffset();
        partitionManager.fail(offset);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Constants.BULK_EVENT_HUB_SPOUT_EMIT_FIELD));
    }

    public BulkEventHubSpout withEventHubSubscriptionKey(String eventHubSubscriptionKey) {
        this.eventHubSubscriptionKey = eventHubSubscriptionKey;
        return this;
    }

    public BulkEventHubSpout withPdsUrl(String pdsUrlForTelemetryData) {
        this.pdsUrlForTelemetryData = pdsUrlForTelemetryData;
        return this;
    }

    public BulkEventHubSpout withHttpProxyHost(String httpProxyHost) {
        this.httpProxyHost = httpProxyHost;
        return this;
    }

    public BulkEventHubSpout wittHttpProxyPort(Integer httpProxyPort) {
        this.httpProxyPort = httpProxyPort;
        return this;
    }

    public BulkEventHubSpout withEventHubDiscoveryResponse(EventHubDiscoveryResponse eventHubDiscoveryResponse) {
        this.eventHubDiscoveryResponse = eventHubDiscoveryResponse;
        this.connectionExpiryTs = eventHubDiscoveryResponse.getConnectionExpiry();
        return this;
    }

    public BulkEventHubSpout build() throws StormAdapterException {

        if (this.eventHubSubscriptionKey == null || this.pdsUrlForTelemetryData == null) {
            throw new StormAdapterException("Subscription Key or PDS URL cannot be null");
        }

        if (this.eventHubDiscoveryResponse != null) {
            return this;
        }
        LOGGER.info("BulkEventHubSpout build is complete");
        return this;
    }

    public Long getConnectionExpiryTs() {
        return connectionExpiryTs;
    }

    public void setConnectionExpiryTs(Long connectionExpiryTs) {
        this.connectionExpiryTs = connectionExpiryTs;
    }

    public EventHubDiscoveryResponse getEventHubDiscoveryResponse() {
        return eventHubDiscoveryResponse;
    }
}