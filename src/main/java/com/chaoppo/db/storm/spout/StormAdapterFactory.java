package com.chaoppo.db.storm.spout;

import com.chaoppo.db.storm.connectivity.DiscoveryClient;
import com.chaoppo.db.storm.error.StormAdapterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class StormAdapterFactory {

    private static final Logger LOG = LoggerFactory.getLogger(StormAdapterFactory.class);
    private DiscoveryClient discoveryClient;
    private Integer bulkMsgCount;
    private String eventHubSubscriptionKey;
    private String pdsUrlForTelemetryData;
    private String httpProxyHost;
    private Integer httpProxyPort;

    public StormAdapterFactory(String eventHubSubscriptionKey, String pdsUrlForTelemetryData) {
        this.eventHubSubscriptionKey = eventHubSubscriptionKey;
        this.pdsUrlForTelemetryData = pdsUrlForTelemetryData;
    }

    public StormAdapterFactory withBulkMsgCount(Integer bulkMsgCount) {
        this.bulkMsgCount = bulkMsgCount;
        return this;
    }

    public StormAdapterFactory withHttpProxyPort(Integer httpProxyPort) {
        this.httpProxyPort = httpProxyPort;
        return this;
    }

    public StormAdapterFactory withHttpProxyHost(String httpProxyHost) {
        this.httpProxyHost = httpProxyHost;
        return this;
    }

    public List<StormDynaSpout> getEventHubSpout() throws StormAdapterException {

        if (eventHubSubscriptionKey == null || eventHubSubscriptionKey.isEmpty()) {
            throw new StormAdapterException("eventHubSubscriptionKey cannot be empty");
        }
        if (pdsUrlForTelemetryData == null || pdsUrlForTelemetryData.isEmpty()) {
            throw new StormAdapterException("partnerDiscoveryUrl cannot be empty");
        }

        if (discoveryClient == null) {
            this.discoveryClient = DiscoveryClient
                    .getInstance(eventHubSubscriptionKey, pdsUrlForTelemetryData, httpProxyHost, httpProxyPort);
        }

        LOG.info("fetching event hubs with supplied information = " + discoveryClient.toString());

        return discoveryClient.getStormDynaSpout(bulkMsgCount);

    }

    public DiscoveryClient getDiscoveryClient() {
        if (discoveryClient == null) {
            this.discoveryClient = DiscoveryClient
                    .getInstance(eventHubSubscriptionKey, pdsUrlForTelemetryData, httpProxyHost, httpProxyPort);
        }
        return discoveryClient;
    }
}
