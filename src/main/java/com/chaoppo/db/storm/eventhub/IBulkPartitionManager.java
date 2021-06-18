package com.chaoppo.db.storm.eventhub;

import org.apache.storm.eventhubs.spout.IPartitionManager;

public interface IBulkPartitionManager extends IPartitionManager {

    BulkEventData receiveBulk();
}
