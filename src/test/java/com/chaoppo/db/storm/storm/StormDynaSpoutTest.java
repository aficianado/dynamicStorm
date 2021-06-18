package com.chaoppo.db.storm.storm;

import com.chaoppo.db.storm.eventhub.EventHubDiscoveryResponse;
import com.chaoppo.db.storm.spout.StormDynaSpout;
import com.chaoppo.db.storm.test.util.EqualsTester;
import com.chaoppo.db.storm.test.util.HashcodeTester;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class StormDynaSpoutTest {

    static StormDynaSpout defaultObj;

    static {
        defaultObj = new StormDynaSpout(null, "spoutName", "region", 1, 1000L);
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

        StormDynaSpout o1 = new StormDynaSpout(null, "spoutName", "region", 1, 1000L);

        EqualsTester.testReflexive(o1);

    }

    @Test
    public void testSymmetric() {

        StormDynaSpout o1 = new StormDynaSpout(null, "spoutName", "region", 1, 1000L);
        StormDynaSpout o2 = new StormDynaSpout(null, "spoutName", "region", 1, 1000L);
        EqualsTester.testSymmetric(o1, o2);
    }

    @Test
    public void testTransitive() {
        StormDynaSpout o1 = new StormDynaSpout(null, "spoutName", "region1", 1, 1000L);
        StormDynaSpout o2 = new StormDynaSpout(null, "spoutName", "region2", 1, 1000L);
        StormDynaSpout o3 = new StormDynaSpout(null, "spoutName", "region3", 1, 1000L);
        EqualsTester.testTransitive(o1, o2, o3);
    }

    @Test
    public void testNonNullity() {
        StormDynaSpout o1 = new StormDynaSpout(null, "spoutName", "region", 1, 1000L);
        EqualsTester.testNonNullity(o1);
    }

    @Test
    public void testDifferent() {
        StormDynaSpout o1 = new StormDynaSpout(null, "spoutName1", "region", 1, 1000L);
        StormDynaSpout o2 = new StormDynaSpout(null, "spoutName2", "region", 1, 1000L);
        EqualsTester.testDifferent(o1, o2);
    }

    @Test
    public void testDifferentObjectClass() {
        StormDynaSpout o1 = new StormDynaSpout(null, "spoutName", "region", 1, 1000L);
        EqualsTester.testDifferentObjectClass(o1, new Object());
    }

    @Test
    public void testNameIsNull() {
        StormDynaSpout o1 = new StormDynaSpout(null, null, "region", 1, 1000L);
        StormDynaSpout o2 = new StormDynaSpout(null, "spoutName", "region", 1, 1000L);
        assertFalse("o1 should not be equals to o2.", o1.equals(o2));
    }

    @Test
    public void testHashCodeConsistency() {
        StormDynaSpout o1 = new StormDynaSpout(null, "spoutName", "region", 1, 1000L);
        HashcodeTester.testHashCodeConsistency(o1);
    }

    @Test
    public void testHashCodeEquality() {
        StormDynaSpout o1 = new StormDynaSpout(null, "spoutName", "region1", 1, 1000L);
        StormDynaSpout o2 = new StormDynaSpout(null, "spoutName", "region2", 1, 1000L);

        HashcodeTester.testHashCodeEquality(o1, o2);

    }

    @Test
    public void testHashCodeInEquality() {
        StormDynaSpout o1 = new StormDynaSpout(null, "spoutName1", "region", 1, 1000L);
        StormDynaSpout o2 = new StormDynaSpout(null, "spoutName2", "region", 1, 1000L);

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

        StormDynaSpout o1 = new StormDynaSpout(null, "spoutName", "region", 1, 1000L);
        o1.setEventHubDiscoveryResponse(ehr);

        assertEquals(o1.getRegion(), defaultObj.getRegion());
        assertEquals(o1.getPartitionCount(), defaultObj.getPartitionCount());
        assertEquals(o1.getSpout(), defaultObj.getSpout());
        assertEquals(o1.getSpoutName(), defaultObj.getSpoutName());
        assertEquals(o1.getTokenExpiry(), defaultObj.getTokenExpiry());

    }
}
