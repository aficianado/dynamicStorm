package com.chaoppo.db.storm.eventhub;

import org.apache.storm.eventhubs.spout.EventHubSpoutConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BulkEventHubSpoutTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testSpoutConfig() {
        EventHubSpoutConfig conf = new EventHubSpoutConfig("username", "pas\\s+w/ord", "namespace", "entityname", 16);
        conf.setZkConnectionString("zookeeper");
        conf.setCheckpointIntervalInSeconds(1);
        assertEquals(conf.getConnectionString(),
                "Endpoint=amqps://namespace.servicebus.windows.net;EntityPath=entityname;SharedAccessKeyName=username;"
                        + "SharedAccessKey=pas\\s+w/ord;OperationTimeout=PT1M;RetryPolicy=Default");
    }

    @Test
    public void testSpoutBasic() {
    }

    @Test
    public void testSpoutCheckpoint() {

    }

}
