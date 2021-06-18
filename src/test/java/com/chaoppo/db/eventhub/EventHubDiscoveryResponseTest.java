package com.chaoppo.db.eventhub;

import com.chaoppo.db.test.util.EqualsTester;
import com.chaoppo.db.test.util.HashcodeTester;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class EventHubDiscoveryResponseTest {

    static EventHubDiscoveryResponse defaultObj = new EventHubDiscoveryResponse();

    static {
        defaultObj.setConnection("connection");
        defaultObj.setConnectionExpiry(1000L);
        defaultObj.setConsumerGroup("consumerGroup");
        defaultObj.setName("name");
        defaultObj.setPartitions("1");
        defaultObj.setRegion("region");
    }

    @Test
    public void testReflexive() {

        EventHubDiscoveryResponse o1 = new EventHubDiscoveryResponse();
        o1.setName("name1");

        EqualsTester.testReflexive(o1);

    }

    @Test
    public void testSymmetric() {

        EventHubDiscoveryResponse o1 = new EventHubDiscoveryResponse();
        o1.setName("name1");
        EventHubDiscoveryResponse o2 = new EventHubDiscoveryResponse();
        o2.setName("name1");
        EqualsTester.testSymmetric(o1, o2);
    }

    @Test
    public void testTransitive() {
        EventHubDiscoveryResponse o1 = new EventHubDiscoveryResponse();
        o1.setName("name1");
        EventHubDiscoveryResponse o2 = new EventHubDiscoveryResponse();
        o2.setName("name1");
        EventHubDiscoveryResponse o3 = new EventHubDiscoveryResponse();
        o3.setName("name1");
        EqualsTester.testTransitive(o1, o2, o3);
    }

    @Test
    public void testNonNullity() {
        EventHubDiscoveryResponse o1 = new EventHubDiscoveryResponse();
        o1.setName("name1");
        EqualsTester.testNonNullity(o1);
    }

    @Test
    public void testDifferent() {
        EventHubDiscoveryResponse o1 = new EventHubDiscoveryResponse();
        o1.setName("name1");
        EventHubDiscoveryResponse o2 = new EventHubDiscoveryResponse();
        o2.setName("name2");
        EqualsTester.testDifferent(o1, o2);
    }

    @Test
    public void testDifferentObjectClass() {
        EventHubDiscoveryResponse o1 = new EventHubDiscoveryResponse();
        o1.setName("name1");

        EqualsTester.testDifferentObjectClass(o1, new Object());
    }

    @Test
    public void testNameIsNull() {
        EventHubDiscoveryResponse o1 = new EventHubDiscoveryResponse();
        o1.setName(null);
        EventHubDiscoveryResponse o2 = new EventHubDiscoveryResponse();
        o2.setName("name1");
        assertFalse("o1 should not be equals to o2.", o1.equals(o2));
    }

    @Test
    public void testHashCodeConsistency() {
        EventHubDiscoveryResponse o1 = new EventHubDiscoveryResponse();
        o1.setName("name1");

        HashcodeTester.testHashCodeConsistency(o1);
    }

    @Test
    public void testHashCodeEquality() {
        EventHubDiscoveryResponse o1 = new EventHubDiscoveryResponse();
        o1.setName("name1");
        EventHubDiscoveryResponse o2 = new EventHubDiscoveryResponse();
        o2.setName("name1");

        HashcodeTester.testHashCodeEquality(o1, o2);

    }

    @Test
    public void testHashCodeInEquality() {
        EventHubDiscoveryResponse o1 = new EventHubDiscoveryResponse();
        o1.setName("name1");
        EventHubDiscoveryResponse o2 = new EventHubDiscoveryResponse();
        o2.setName("name2");

        HashcodeTester.testHashCodeEquality(o1, o2);

    }

    @Test
    public void testGetResponse() {
        EventHubDiscoveryResponse o1 = new EventHubDiscoveryResponse();
        o1.setName("name");
        o1.setConnection("connection");
        o1.setConnectionExpiry(1000L);
        o1.setConsumerGroup("consumerGroup");
        o1.setPartitions("1");
        o1.setRegion("region");

        assertEquals(o1.getConnection(), defaultObj.getConnection());
        assertEquals(o1.getConnectionExpiry(), defaultObj.getConnectionExpiry());
        assertEquals(o1.getConsumerGroup(), defaultObj.getConsumerGroup());
        assertEquals(o1.getName(), defaultObj.getName());
        assertEquals(o1.getPartitions(), defaultObj.getPartitions());
        assertEquals(o1.getRegion(), defaultObj.getRegion());

    }

    @Test
    public void testToString() {
        assertNotNull("toString should not be null", defaultObj.toString());
    }
}
