package com.chaoppo.db.storm.spout;

import com.chaoppo.db.storm.eventhub.EventHubDiscoveryResponse;
import org.apache.storm.topology.base.BaseRichSpout;

public class StormDynaSpout {

    private BaseRichSpout spout;
    private String spoutName;
    private Integer partitionCount;
    private Long tokenExpiry;
    private String region;
    private EventHubDiscoveryResponse eventHubDiscoveryResponse;

    public StormDynaSpout(BaseRichSpout spout, String spoutName, String region, Integer partitionCount,
            long tokenExpiry) {
        this.spout = spout;
        this.spoutName = spoutName;
        this.partitionCount = partitionCount;
        this.tokenExpiry = tokenExpiry;
        this.region = region;
    }

    public BaseRichSpout getSpout() {
        return spout;
    }

    public void setSpout(BaseRichSpout spout) {
        this.spout = spout;
    }

    public String getSpoutName() {
        return spoutName;
    }

    public void setSpoutName(String spoutName) {
        this.spoutName = spoutName;
    }

    public Integer getPartitionCount() {
        return partitionCount;
    }

    public void setPartitionCount(Integer partitionCount) {
        this.partitionCount = partitionCount;
    }

    public Long getTokenExpiry() {
        return tokenExpiry;
    }

    public void setTokenExpiry(Long tokenExpiry) {
        this.tokenExpiry = tokenExpiry;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public EventHubDiscoveryResponse getEventHubDiscoveryResponse() {
        return eventHubDiscoveryResponse;
    }

    public void setEventHubDiscoveryResponse(EventHubDiscoveryResponse eventHubDiscoveryResponse) {
        this.eventHubDiscoveryResponse = eventHubDiscoveryResponse;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((spoutName == null) ? 0 : spoutName.hashCode());
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
        StormDynaSpout other = (StormDynaSpout) obj;
        if (spoutName == null) {
            if (other.spoutName != null) {
                return false;
            }
        } else if (!spoutName.equals(other.spoutName)) {
            return false;
        }
        return true;
    }

}
