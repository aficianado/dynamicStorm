package com.chaoppo.db.storm;

import com.chaoppo.db.eventhub.EventHubDiscoveryResponse;
import com.chaoppo.db.test.util.EqualsTester;
import com.chaoppo.db.test.util.HashcodeTester;
import com.chaoppo.db.util.StormSpout;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class StormDynaSpoutTest {

    static StormSpout defaultObj;

    static {
        defaultObj = new StormSpout(null, "spoutName", "region", 1, 1000L);
        EventHubDiscoveryResponse ehr = new EventHubDiscoveryResponse();
        ehr.setConnection("connection");
        ehr.setConnectionExpiry(1000L);
        ehr.setConsumerGroup("consumerGroup");
        ehr.setName("name");
        ehr.setPartitions("1");
        ehr.setRegion("region");
        defaultObj.setRegion("region");
        defaultObj.setEventHubDiscoveryResponse(ehr);
    }

    @Test
    public void testReflexive() {

        StormSpout o1 = new StormSpout(null, "spoutName", "region", 1, 1000L);

        EqualsTester.testReflexive(o1);

    }

    @Test
    public void testSymmetric() {

        StormSpout o1 = new StormSpout(null, "spoutName", "region", 1, 1000L);
        StormSpout o2 = new StormSpout(null, "spoutName", "region", 1, 1000L);
        EqualsTester.testSymmetric(o1, o2);
    }

    @Test
    public void testTransitive() {
        StormSpout o1 = new StormSpout(null, "spoutName", "region1", 1, 1000L);
        StormSpout o2 = new StormSpout(null, "spoutName", "region2", 1, 1000L);
        StormSpout o3 = new StormSpout(null, "spoutName", "region3", 1, 1000L);
        EqualsTester.testTransitive(o1, o2, o3);
    }

    @Test
    public void testNonNullity() {
        StormSpout o1 = new StormSpout(null, "spoutName", "region", 1, 1000L);
        EqualsTester.testNonNullity(o1);
    }

    @Test
    public void testDifferent() {
        StormSpout o1 = new StormSpout(null, "spoutName1", "region", 1, 1000L);
        StormSpout o2 = new StormSpout(null, "spoutName2", "region", 1, 1000L);
        EqualsTester.testDifferent(o1, o2);
    }

    @Test
    public void testDifferentObjectClass() {
        StormSpout o1 = new StormSpout(null, "spoutName", "region", 1, 1000L);
        EqualsTester.testDifferentObjectClass(o1, new Object());
    }

    @Test
    public void testNameIsNull() {
        StormSpout o1 = new StormSpout(null, null, "region", 1, 1000L);
        StormSpout o2 = new StormSpout(null, "spoutName", "region", 1, 1000L);
        assertFalse("o1 should not be equals to o2.", o1.equals(o2));
    }

    @Test
    public void testHashCodeConsistency() {
        StormSpout o1 = new StormSpout(null, "spoutName", "region", 1, 1000L);
        HashcodeTester.testHashCodeConsistency(o1);
    }

    @Test
    public void testHashCodeEquality() {
        StormSpout o1 = new StormSpout(null, "spoutName", "region1", 1, 1000L);
        StormSpout o2 = new StormSpout(null, "spoutName", "region2", 1, 1000L);

        HashcodeTester.testHashCodeEquality(o1, o2);

    }

    @Test
    public void testHashCodeInEquality() {
        StormSpout o1 = new StormSpout(null, "spoutName1", "region", 1, 1000L);
        StormSpout o2 = new StormSpout(null, "spoutName2", "region", 1, 1000L);

        HashcodeTester.testHashCodeEquality(o1, o2);

    }

    @Test
    public void testGet() {

        EventHubDiscoveryResponse ehr = new EventHubDiscoveryResponse();
        ehr.setConnection("connection");
        ehr.setConnectionExpiry(1000L);
        ehr.setConsumerGroup("consumerGroup");
        ehr.setName("name");
        ehr.setPartitions("1");
        ehr.setRegion("region");

        StormSpout o1 = new StormSpout(null, "spoutName", "region", 1, 1000L);
        o1.setEventHubDiscoveryResponse(ehr);

        assertEquals(o1.getRegion(), defaultObj.getRegion());
        assertEquals(o1.getPartitionCount(), defaultObj.getPartitionCount());
        assertEquals(o1.getSpout(), defaultObj.getSpout());
        assertEquals(o1.getSpoutName(), defaultObj.getSpoutName());
        assertEquals(o1.getTokenExpiry(), defaultObj.getTokenExpiry());

    }
}
