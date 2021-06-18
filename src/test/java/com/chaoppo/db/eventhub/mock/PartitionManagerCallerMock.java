package com.chaoppo.db.eventhub.mock;

import com.chaoppo.db.eventhub.BulkEventData;
import com.chaoppo.db.storm.eventhub.BulkPartitionManager;
import com.chaoppo.db.storm.eventhub.IBulkPartitionManager;
import org.apache.storm.eventhubs.spout.EventHubSpoutConfig;
import org.apache.storm.eventhubs.spout.IEventDataScheme;
import org.apache.storm.eventhubs.spout.IStateStore;

public class PartitionManagerCallerMock {

    public static final String statePath = "/eventhubspout/TestTopo/namespace/entityname/partitions/1";
    private IBulkPartitionManager pm;
    private IStateStore stateStore;

    public PartitionManagerCallerMock(String partitionId, IEventDataScheme scheme) {
        this(partitionId, 0, scheme);
    }

    public PartitionManagerCallerMock(String partitionId, long enqueueTimeFilter, IEventDataScheme scheme) {
        EventHubReceiverMock receiver = new EventHubReceiverMock(partitionId);
        EventHubSpoutConfig conf = new EventHubSpoutConfig("username", "password", "namespace", "entityname", 16,
                "zookeeper", 10, 1024, 1024, enqueueTimeFilter);
        conf.setEventDataScheme(scheme);
        conf.setTopologyName("TestTopo");
        stateStore = new StateStoreMock();
        this.pm = new BulkPartitionManager(conf, partitionId, stateStore, receiver, 1);

        stateStore.open();
        try {
            pm.open();
        } catch (Exception ex) {
        }
    }

    /**
     * Execute a sequence of calls to Partition Manager.
     *
     * @param callSequence: is represented as a string of commands, e.g. "r,r,r,r,a1,f2,...". The commands are: r[N]:
     *                      receive() called N times aX: ack(X) fY: fail(Y)
     * @return the sequence of messages the receive call returns
     */
    public String execute(String callSequence) {

        String[] cmds = callSequence.split(",");
        StringBuilder ret = new StringBuilder();
        for (String cmd : cmds) {
            if (cmd.startsWith("r")) {
                int count = 1;
                if (cmd.length() > 1) {
                    count = Integer.parseInt(cmd.substring(1));
                }
                for (int i = 0; i < count; ++i) {
                    BulkEventData ed = pm.receiveBulk();
                    if (ed == null) {
                        ret.append("null,");
                    } else {
                        ret.append(ed.getMessageId().getOffset());
                        ret.append(",");
                    }
                }
            } else if (cmd.startsWith("a")) {
                pm.ack(cmd.substring(1));
            } else if (cmd.startsWith("f")) {
                pm.fail(cmd.substring(1));
            }
        }
        if (ret.length() > 0) {
            ret.setLength(ret.length() - 1);
        }
        return ret.toString();
    }

    /**
     * Exercise the IPartitionManager.checkpoint() method
     *
     * @return the offset that we write to state store
     */
    public String checkpoint() {
        pm.checkpoint();
        return stateStore.readData(statePath);
    }
}
