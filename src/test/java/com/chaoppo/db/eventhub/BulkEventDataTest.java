package com.chaoppo.db.eventhub;

import com.chaoppo.db.storm.eventhub.BulkEventData;
import com.chaoppo.db.test.util.EqualsTester;
import com.chaoppo.db.test.util.HashcodeTester;
import org.apache.storm.eventhubs.spout.MessageId;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BulkEventDataTest {

    @Test
    public void testEventDataComparision() {

        MessageId messageId1 = MessageId.create(null, "3", 1);
        BulkEventData eventData1 = BulkEventData.create(null, messageId1);

        MessageId messageId2 = MessageId.create(null, "13", 2);
        BulkEventData eventData2 = BulkEventData.create(null, messageId2);

        assertTrue(eventData2.compareTo(eventData1) > 0);
    }

    @Test
    public void testReflexive() {

        MessageId messageId1 = MessageId.create(null, "3", 1);
        BulkEventData eventData1 = BulkEventData.create(null, messageId1);

        EqualsTester.testReflexive(eventData1);

    }

    @Test
    public void testSymmetric() {

        MessageId messageId1 = MessageId.create(null, "3", 1);
        BulkEventData eventData1 = BulkEventData.create(null, messageId1);

        BulkEventData eventData2 = BulkEventData.create(null, messageId1);
        EqualsTester.testSymmetric(eventData1, eventData2);
    }

    @Test
    public void testTransitive() {
        MessageId messageId1 = MessageId.create(null, "3", 1);
        BulkEventData o1 = BulkEventData.create(null, messageId1);
        BulkEventData o2 = BulkEventData.create(null, messageId1);
        BulkEventData o3 = BulkEventData.create(null, messageId1);
        EqualsTester.testTransitive(o1, o2, o3);
    }

    @Test
    public void testNonNullity() {
        MessageId messageId1 = MessageId.create(null, "3", 1);
        BulkEventData o1 = BulkEventData.create(null, messageId1);
        EqualsTester.testNonNullity(o1);
    }

    @Test
    public void testDifferent() {
        MessageId messageId1 = MessageId.create(null, "1", 1);
        BulkEventData o1 = BulkEventData.create(null, messageId1);

        MessageId messageId2 = MessageId.create(null, "2", 2);
        BulkEventData o2 = BulkEventData.create(null, messageId2);
        EqualsTester.testDifferent(o1, o2);
    }

    @Test
    public void testDifferentObjectClass() {
        MessageId messageId1 = MessageId.create(null, "1", 1);
        BulkEventData o1 = BulkEventData.create(null, messageId1);

        EqualsTester.testDifferentObjectClass(o1, new Object());
    }

    @Test
    public void testMessageIdIsNullClass() {
        BulkEventData o1 = new BulkEventData(null, null);

        MessageId messageId1 = MessageId.create(null, "1", 1);
        BulkEventData o2 = BulkEventData.create(null, messageId1);
        assertFalse("o1 should not be equals to o2.", o1.equals(o2));
    }

    @Test
    public void testHashCodeConsistency() {
        MessageId messageId1 = MessageId.create(null, "1", 1);
        BulkEventData o1 = BulkEventData.create(null, messageId1);

        HashcodeTester.testHashCodeConsistency(o1);
    }

    @Test
    public void testHashCodeEquality() {
        MessageId messageId1 = MessageId.create(null, "1", 1);
        BulkEventData o1 = BulkEventData.create(null, messageId1);

        MessageId messageId2 = MessageId.create(null, "1", 2);
        BulkEventData o2 = BulkEventData.create(null, messageId2);

        HashcodeTester.testHashCodeEquality(o1, o2);

    }

    @Test
    public void testGetMessages() {
        MessageId messageId1 = MessageId.create(null, "1", 1);
        BulkEventData o1 = new BulkEventData(null, messageId1);

        assertNull("getMessage should return null", o1.getMessages());
    }

}
