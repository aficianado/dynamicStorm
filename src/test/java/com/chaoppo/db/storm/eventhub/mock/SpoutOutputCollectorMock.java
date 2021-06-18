package com.chaoppo.db.storm.eventhub.mock;

import org.apache.storm.eventhubs.spout.MessageId;
import org.apache.storm.spout.ISpoutOutputCollector;

import java.util.List;

public class SpoutOutputCollectorMock implements ISpoutOutputCollector {

    StringBuilder emittedOffset;

    public SpoutOutputCollectorMock() {
        emittedOffset = new StringBuilder();
    }

    public String getOffsetSequenceAndReset() {
        String ret = null;
        if (emittedOffset.length() > 0) {
            emittedOffset.setLength(emittedOffset.length() - 1);
            ret = emittedOffset.toString();
            emittedOffset.setLength(0);
        }
        return ret;
    }

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
        MessageId mid = (MessageId) messageId;
        String pid = mid.getPartitionId();
        String offset = mid.getOffset();
        emittedOffset.append(pid + "_" + offset + ",");
        return null;
    }

    @Override
    public void emitDirect(int arg0, String arg1, List<Object> arg2, Object arg3) {
    }

    //	@Override
    //	public void flush() {
    //		// NO-OP
    //	}

    @Override
    public long getPendingCount() {
        return 0;
    }

    @Override
    public void reportError(Throwable arg0) {
    }
}
