package com.chaoppo.db.storm.eventhub;

import java.io.Serializable;

public class EventHubDiscoveryResponse implements Serializable {

    private static final long serialVersionUID = -4934544625984118543L;

    private String name;
    private String connection;
    private Long connectionExpiry;
    private String consumerGroup;
    private String partitions;
    private String region;

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getConnection() {
        return this.connection;
    }

    public void setConnection(String connection) {
        this.connection = connection;
    }

    public Long getConnectionExpiry() {
        return this.connectionExpiry;
    }

    public void setConnectionExpiry(Long connectionExpiry) {
        this.connectionExpiry = connectionExpiry;
    }

    public String getConsumerGroup() {
        return this.consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getPartitions() {
        return this.partitions;
    }

    public void setPartitions(String partitions) {
        this.partitions = partitions;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((region == null) ? 0 : region.hashCode());
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
        EventHubDiscoveryResponse other = (EventHubDiscoveryResponse) obj;
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }
        if (region == null) {
            if (other.region != null) {
                return false;
            }
        } else if (!region.equals(other.region)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "EventHubDiscoveryResponse [name=" + name + ", connection=" + connection + ", connectionExpiry="
                + connectionExpiry + ", consumerGroup=" + consumerGroup + ", partitions=" + partitions + ", region="
                + region + "]";
    }

}
