package com.chaoppo.db.eventhub;

import com.chaoppo.db.eventhub.mock.PartitionManagerCallerMock;
import org.apache.storm.eventhubs.spout.EventDataScheme;
import org.apache.storm.eventhubs.spout.IEventDataScheme;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BulkPartitionManagerTest {

    private IEventDataScheme eventDataScheme = new EventDataScheme();

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testPartitionManagerNoFail() {
        PartitionManagerCallerMock mock = new PartitionManagerCallerMock("1", eventDataScheme);
        String result = mock.execute("r,r,r,a0,a1,a2,r");
        assertEquals("0,1,2,3", result);
    }

    @Test
    public void testPartitionManagerResend() {
        PartitionManagerCallerMock mock = new PartitionManagerCallerMock("1", eventDataScheme);
        String result = mock.execute("r,a0,r,r,r,f3,r,f2,f1,r,r,a1,a2,a3,r");
        assertEquals("0,1,2,3,3,1,2,4", result);
    }

    @Test
    public void testPMCheckpointWithPending() {
        PartitionManagerCallerMock mock = new PartitionManagerCallerMock("1", eventDataScheme);
        mock.execute("r,r,r");
        // no ack, so return the first of pending list
        assertEquals("0", mock.checkpoint());
        mock.execute("a0,a2");
        // still need to return the first of pending list
        assertEquals("1", mock.checkpoint());
    }

    @Test
    public void testPMCheckpointWithResend() {
        PartitionManagerCallerMock mock = new PartitionManagerCallerMock("1", eventDataScheme);
        mock.execute("r,r,r,f2,f1,f0");
        // pending is empty, return the smallest in toResend
        assertEquals("0", mock.checkpoint());
        mock.execute("r,a0");
        // pending is still empty
        assertEquals("1", mock.checkpoint());
    }

    @Test
    public void testPMCheckpointWithPendingAndResend() {
        PartitionManagerCallerMock mock = new PartitionManagerCallerMock("1", eventDataScheme);
        mock.execute("r,r,r,f2,f1");
        // return the smaller of pending and toResend
        assertEquals("0", mock.checkpoint());
        mock.execute("a0,r");
        // now pending: [3], toResend: [1,2]
        assertEquals("1", mock.checkpoint());
    }

    @Test
    public void testPMCheckpointWithNoPendingAndNoResend() {
        PartitionManagerCallerMock mock = new PartitionManagerCallerMock("1", eventDataScheme);
        // if no event sent, no checkpoint shall be created
        assertEquals(null, mock.checkpoint());
        mock.execute("r,r,r,f2,f1,r,r,a2,a1,a0");
        // all events are sent successfully, return last sent offset
        assertEquals("2", mock.checkpoint());
    }

    @Test
    public void testPartitionManagerEnqueueTimeFilter() {
        PartitionManagerCallerMock mock = new PartitionManagerCallerMock("1", 123456, eventDataScheme);
        String result = mock.execute("r2");
        assertEquals("123457,123458", result);
    }
}
