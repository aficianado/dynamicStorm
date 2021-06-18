package com.chaoppo.db.storm.eventhub;

import com.microsoft.azure.eventhubs.EventData;
import org.apache.storm.eventhubs.spout.MessageId;

import java.util.List;

public class BulkEventData implements Comparable<BulkEventData> {

    private final List<EventData> messages;
    private final MessageId messageId;

    public BulkEventData(List<EventData> messages, MessageId messageId) {
        this.messages = messages;
        this.messageId = messageId;
    }

    public static BulkEventData create(List<EventData> message, MessageId messageId) {
        return new BulkEventData(message, messageId);
    }

    public List<EventData> getMessages() {
        return this.messages;
    }

    @Override
    public int compareTo(BulkEventData ed) {
        return messageId.getSequenceNumber().compareTo(ed.getMessageId().getSequenceNumber());
    }

    public MessageId getMessageId() {
        return this.messageId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((messageId == null) ? 0 : messageId.getSequenceNumber().hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        BulkEventData other = (BulkEventData) obj;
        if (messageId == null) {
            if (other.messageId != null) {
                return false;
            }
        } else if (!messageId.getSequenceNumber().equals(other.messageId.getSequenceNumber())) {
            return false;
        }
        return true;
    }
}
