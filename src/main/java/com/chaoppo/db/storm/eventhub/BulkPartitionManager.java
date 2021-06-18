package com.chaoppo.db.storm.eventhub;

import com.microsoft.azure.eventhubs.EventData;
import org.apache.storm.eventhubs.spout.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

public class BulkPartitionManager extends SimplePartitionManager implements IBulkPartitionManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(BulkPartitionManager.class);
    private static final int EH_RECEIVER_TIMEOUT_MILLI_SECS = 200;

    // all sent events are stored in pending
    private final Map<String, BulkEventData> pending;
    // all failed events are put in toResend, which is sorted by event's offset
    private final TreeSet<BulkEventData> toResend;
    private final String partitionId;
    private final int bulkMessageCount;

    public BulkPartitionManager(EventHubSpoutConfig spoutConfig, String partitionId, IStateStore stateStore,
            IEventHubReceiver receiver, int bulkMessageCount) {
        super(spoutConfig, partitionId, stateStore, receiver);
        this.pending = new LinkedHashMap<>();
        this.toResend = new TreeSet<>();
        this.partitionId = partitionId;
        this.bulkMessageCount = bulkMessageCount;
    }

    @Override
    protected String getCompletedOffset() {
        String offset = null;

        if (pending != null && !pending.isEmpty()) {
            // find the smallest offset in pending list
            offset = pending.keySet().iterator().next();
        }
        if (toResend != null && !toResend.isEmpty()) {
            // find the smallest offset in toResend list
            String offset2 = toResend.first().getMessageId().getOffset();
            if (offset == null || offset2.compareTo(offset) < 0) {
                offset = offset2;
            }
        }
        if (offset == null) {
            offset = lastOffset;
        }
        return offset;
    }

    @Override
    public EventDataWrap receive() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void ack(String offset) {
        pending.remove(offset);
    }

    @Override
    public void fail(String offset) {
        LOGGER.warn("fail on " + offset);
        BulkEventData eventData = pending.remove(offset);
        toResend.add(eventData);
    }

    @Override
    public BulkEventData receiveBulk() {
        BulkEventData eventData;
        if (toResend.isEmpty()) {
            eventData = receiveBatch(bulkMessageCount);
        } else {
            LOGGER.info("Processing existing msgs received " + toResend.size());
            eventData = toResend.pollFirst();
        }
        if (eventData != null) {
            lastOffset = eventData.getMessageId().getOffset();
            pending.put(lastOffset, eventData);
        }
        return eventData;
    }

    private BulkEventData receiveBatch(int count) {

        List<EventData> messages = new ArrayList<>();
        MessageId lastMessageId = null;
        long ttime = System.currentTimeMillis();
        for (int i = 0; i < count && (System.currentTimeMillis() - ttime) < EH_RECEIVER_TIMEOUT_MILLI_SECS; ++i) {

            EventDataWrap ed = receiver.receive();
            if (ed != null) {

                lastMessageId = ed.getMessageId();
                EventData eventData = ed.getEventData();

                if (eventData == null) {
                    continue;
                }

                byte[] data = eventData.getBytes();
                Object amqpMsg = eventData.getObject();
                if (data != null || amqpMsg != null) {
                    messages.add(eventData);
                }
            }
        }
        if (messages.isEmpty()) {
            return null;
        }
        LOGGER.info(String.format("BulkEventData id %s count %d, offset %s", partitionId, messages.size(),
                lastMessageId != null ? lastMessageId.getOffset() : "null"));
        return new BulkEventData(messages, lastMessageId);
    }
}