package com.chaoppo.db.storm.eventhub.mock;

import com.microsoft.azure.eventhubs.EventData;

import java.util.Map;

public class EventHubReceiverMock implements IEventHubReceiver {

    private static boolean isPaused = false;
    private final String partitionId;
    private long currentOffset;
    private boolean isOpen;

    public EventHubReceiverMock(String pid) {
        partitionId = pid;
        isPaused = false;
    }

    /**
     * Use this method to pause/resume all the receivers. If paused all receiver will return null.
     *
     * @param val
     */
    public static void setPause(boolean val) {
        isPaused = val;
    }

    @Override
    public void open(IEventFilter filter) throws EventHubException {
        currentOffset =
                filter.getOffset() != null ? Long.parseLong(filter.getOffset()) : filter.getTime().toEpochMilli();
        isOpen = true;
    }

    @Override
    public void close() {
        isOpen = false;
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public EventDataWrap receive() {
        if (isPaused) {
            return null;
        }

        currentOffset++;

        // the body of the message is "message" + currentOffset, e.g. "message123"

        MessageId mid = new MessageId(partitionId, "" + currentOffset, currentOffset);
        EventData ed = new EventData(("message" + currentOffset).getBytes());
        return EventDataWrap.create(ed, mid);
    }

    @Override
    public Map<String, Object> getMetricsData() {
        return null;
    }
}
