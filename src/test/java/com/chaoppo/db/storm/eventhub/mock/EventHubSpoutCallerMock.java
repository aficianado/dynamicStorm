package com.chaoppo.db.storm.eventhub.mock;

import org.apache.storm.spout.SpoutOutputCollector;

public class EventHubSpoutCallerMock {

    public static final String statePathPrefix = "/eventhubspout/TestTopo/namespace/entityname/partitions/";
    EventHubSpout spout;
    private IStateStore stateStore;
    private SpoutOutputCollectorMock collector;

    public EventHubSpoutCallerMock(int totalPartitions, int totalTasks, int taskIndex, int checkpointInterval) {
        stateStore = new StateStoreMock();
        EventHubSpoutConfig conf = new EventHubSpoutConfig("username", "password", "namespace", "entityname",
                totalPartitions, "zookeeper", checkpointInterval, 1024);
        conf.setTopologyName("TestTopo");

        IEventHubReceiverFactory recvFactory = new IEventHubReceiverFactory() {

            @Override
            public IEventHubReceiver create(EventHubSpoutConfig config, String partitionId) {
                return new EventHubReceiverMock(partitionId);
            }
        };
        // mock state store and receiver
        spout = new EventHubSpout(conf, stateStore, null, recvFactory);

        collector = new SpoutOutputCollectorMock();

        try {
            spout.preparePartitions(null, totalTasks, taskIndex, new SpoutOutputCollector(collector));
        } catch (Exception ex) {
        }
    }

    /**
     * Execute a sequence of calls to EventHubSpout.
     *
     * @param callSequence: is represented as a string of commands, e.g. "r,r,r,r,a1,f2,...". The commands are: r[N]:
     *                      receive() called N times aP_X: ack(P_X), partition: P, offset: X fP_Y: fail(P_Y), partition:
     *                      P, offset: Y
     */
    public String execute(String callSequence) {
        String[] cmds = callSequence.split(",");
        for (String cmd : cmds) {
            if (cmd.startsWith("r")) {
                int count = 1;
                if (cmd.length() > 1) {
                    count = Integer.parseInt(cmd.substring(1));
                }
                for (int i = 0; i < count; ++i) {
                    spout.nextTuple();
                }
            } else if (cmd.startsWith("a")) {
                String[] midStrs = cmd.substring(1).split("_");
                MessageId msgId = new MessageId(midStrs[0], midStrs[1], Long.parseLong(midStrs[1]));
                spout.ack(msgId);
            } else if (cmd.startsWith("f")) {
                String[] midStrs = cmd.substring(1).split("_");
                MessageId msgId = new MessageId(midStrs[0], midStrs[1], Long.parseLong(midStrs[1]));
                spout.fail(msgId);
            }
        }
        return collector.getOffsetSequenceAndReset();
    }

    public String getCheckpoint(int partitionIndex) {
        String statePath = statePathPrefix + partitionIndex;
        return stateStore.readData(statePath);
    }
}
