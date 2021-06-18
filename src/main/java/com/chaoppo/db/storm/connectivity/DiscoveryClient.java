package com.chaoppo.db.storm.connectivity;

import com.chaoppo.db.storm.error.StormAdapterException;
import com.chaoppo.db.storm.eventhub.EventHubDiscoveryResponse;
import com.chaoppo.db.storm.http.HttpClient;
import com.chaoppo.db.storm.http.HttpMethod;
import com.chaoppo.db.storm.http.Request;
import com.chaoppo.db.storm.http.Response;
import com.chaoppo.db.storm.spout.BulkEventHubSpout;
import com.chaoppo.db.storm.spout.StormDynaSpout;
import com.chaoppo.db.storm.util.Constants;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.eventhubs.spout.EventHubSpoutConfig;
import org.apache.storm.shade.org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.net.*;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;

public class DiscoveryClient implements Serializable {

    private static final long serialVersionUID = 8564295658087453835L;

    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryClient.class);

    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
    private static final Long RETRY_INTERVAL_SECONDS = 5l;
    private static DiscoveryClient instance;
    private final String eventHubSubscriptionKey;
    private final String pdsUrlForTelemetryData;
    private final String httpProxyHost;
    private final Integer httpProxyPort;

    private DiscoveryClient(String eventHubSubscriptionKey, String pdsUrlForTelemetryData, String httpProxyHost,
            Integer httpProxyPort) {
        this.eventHubSubscriptionKey = eventHubSubscriptionKey;
        this.pdsUrlForTelemetryData = pdsUrlForTelemetryData;
        this.httpProxyHost = httpProxyHost;
        this.httpProxyPort = httpProxyPort;

        if (httpProxyHost != null && httpProxyPort != null) {
            loadProxy(this.httpProxyHost, this.httpProxyPort);
        }
    }

    public static final void loadProxy(String httpProxyHost, int httpProxyPort) {
        if (httpProxyHost != null) {
            ProxySelector.setDefault(new ProxySelector() {

                @Override
                public List<Proxy> select(URI uri) {
                    LinkedList<Proxy> proxies = new LinkedList<>();
                    proxies.add(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(httpProxyHost, httpProxyPort)));
                    return proxies;
                }

                @Override
                public void connectFailed(URI uri, SocketAddress sa, IOException ioe) {
                    LOG.info("connectFailed not supported");
                }
            });
        }
    }

    public static synchronized DiscoveryClient getInstance(String eventHubSubscriptionKey, String pdsUrl,
            String httpProxyHost, Integer httpProxyPort) {
        if (instance == null) {
            if (eventHubSubscriptionKey.startsWith("mock")) {
                return mock(DiscoveryClient.class);
            }

            instance = new DiscoveryClient(eventHubSubscriptionKey, pdsUrl, httpProxyHost, httpProxyPort);
        }
        return instance;
    }

    public static EventHubSpoutConfig generateEventHubSpoutConfig(EventHubDiscoveryResponse eventHubDiscoveryResponse) {
        final ConnectionStringBuilder connectionStringBuilder = new ConnectionStringBuilder(
                eventHubDiscoveryResponse.getConnection());
        String hostName = connectionStringBuilder.getEndpoint().getHost().replaceAll(".servicebus.windows.net", "");
        return new EventHubSpoutConfig(connectionStringBuilder.getSasKeyName(), connectionStringBuilder.getSasKey(),
                hostName, eventHubDiscoveryResponse.getName(),
                Integer.parseInt(eventHubDiscoveryResponse.getPartitions()))
                .withConsumerGroupName(eventHubDiscoveryResponse.getConsumerGroup());
    }

    @Override
    public String toString() {
        return "DiscoveryClient [eventHubSubscriptionKey=" + eventHubSubscriptionKey + ", pdsUrl="
                + pdsUrlForTelemetryData + ", httpProxyHost=" + httpProxyHost + ", httpProxyPort=" + httpProxyPort
                + "]";
    }

    public List<StormDynaSpout> getStormDynaSpout(Integer bulkMsgCount) throws StormAdapterException {

        LOG.info("getStormDynaSpout started");
        List<StormDynaSpout> eventHubSet = new ArrayList<>();
        List<EventHubDiscoveryResponse> response = retrieveDiscoveryResponse();

        if (response == null || response.isEmpty()) {
            return eventHubSet;
        }

        if (bulkMsgCount == null || bulkMsgCount <= 0) {
            bulkMsgCount = 20;
        }

        for (EventHubDiscoveryResponse res : response) {

            BulkEventHubSpout bulkEventHubSpout = new BulkEventHubSpout(bulkMsgCount)
                    .withEventHubSubscriptionKey(this.eventHubSubscriptionKey).withPdsUrl(this.pdsUrlForTelemetryData)
                    .withHttpProxyHost(this.httpProxyHost).wittHttpProxyPort(this.httpProxyPort)
                    .withEventHubDiscoveryResponse(res).build();
            StormDynaSpout stormSpout = new StormDynaSpout(bulkEventHubSpout, res.getName(), res.getRegion(),
                    Integer.parseInt(res.getPartitions()), res.getConnectionExpiry());
            stormSpout.setEventHubDiscoveryResponse(res);
            eventHubSet.add(stormSpout);
        }

        LOG.info("getStormDynaSpout end");
        return eventHubSet;
    }

    public List<EventHubDiscoveryResponse> retrieveDiscoveryResponse() throws StormAdapterException {
        LOG.info("retrieving Discover Response");

        List<EventHubDiscoveryResponse> responseList = null;

        LOG.info("Invoking partner discovery for = [{}]", this.pdsUrlForTelemetryData);
        int count = 0;
        int maxRetry = 3;
        do {
            try {
                Response response = prepareRequestAndExecute(this.pdsUrlForTelemetryData, this.eventHubSubscriptionKey,
                        String.format("%05d", new SecureRandom().nextInt(100000)), null, null, null, HttpMethod.GET);

                LOG.info("Partner discovery http response code for event hub [{}, retryCount = {}]: ",
                        response.getCode(), count);
                if (response.isSuccessful()) {

                    responseList = getEventHubResponse(response);
                    LOG.info("Partner discovery response is success EventHubDiscoveryResponse = " + responseList);
                    return responseList;
                } else {
                    ++count;

                    LOG.error("Unsuccessful request made to partner discovery service. "
                            + "Response from the stream is [{}] , response code is [{}]", response.body());
                    LOG.error(Constants.EXCEPTION_MSG, response.getCode());
                }
            } catch (Exception e) {

                sleep(RETRY_INTERVAL_SECONDS);

                if (++count >= maxRetry) {
                    LOG.error("Retry count exahusted. Partner Discovery Response is unscuccessfull::" + ExceptionUtils
                            .getFullStackTrace(e));
                    throw new StormAdapterException(ExceptionUtils.getStackTrace(e));
                }
                LOG.error("Exception occured in Partner Discovery Response, Retrying again:: retry count = " + count
                        + " Exception = " + ExceptionUtils.getFullStackTrace(e));

            }
        } while (count <= maxRetry);

        // trim response
        lastKeyTrimFromConnectionString(responseList);

        return responseList;
    }

    private void lastKeyTrimFromConnectionString(List<EventHubDiscoveryResponse> responseList) {
        if (responseList != null && !responseList.isEmpty()) {
            responseList.forEach(r -> {
                String connection = r.getConnection();
                if (connection != null && !connection.isEmpty() && connection
                        .substring(connection.length() - 1, connection.length()).equals(";")) {
                    r.setConnection(connection.substring(0, connection.length() - 1));
                }
            });
        }
    }

    private void sleep(Long secs) {
        try {
            TimeUnit.SECONDS.sleep(secs);
        } catch (InterruptedException e1) {
            LOG.info("InterruptedException while trying to sleep for [" + secs + "] sec; " + ExceptionUtils
                    .getFullStackTrace(e1));
            Thread.currentThread().interrupt();
        }
    }

    public static List<EventHubDiscoveryResponse> getEventHubResponse(Response response) {
        String ehConnectionString = response.body();
        List<EventHubDiscoveryResponse> responseList;
        try {
            Type listType = new TypeToken<ArrayList<EventHubDiscoveryResponse>>() {

            }.getType();
            responseList = GSON.fromJson(ehConnectionString, listType);
        } catch (Exception e) {
            Type objType = new TypeToken<EventHubDiscoveryResponse>() {

            }.getType();
            responseList = ImmutableList.of(GSON.fromJson(ehConnectionString, objType));
        }
        return responseList;

    }

    public static Response prepareRequestAndExecute(String url, String subscriptionKey, String trackingId,
            String contentType, String body, String metadata, HttpMethod method) throws StormAdapterException {
        try {

            LOG.info("Invoking prepareRequestAndExecute with parameter [{}, {}, {}, {}, {}, {}]", url, subscriptionKey,
                    trackingId, contentType, body, metadata);

            Request request = new Request(url).addHeader(Constants.OCP_APIM_SUB_KEY, subscriptionKey)
                    .addHeader(Constants.TRACKING_ID, trackingId)
                    .addHeader(Constants.USER_AGENT, Constants.STORM_DYNA_ADAPTER).addMethod(method);

            if (contentType != null && !contentType.isEmpty()) {
                request.addHeader(Constants.CONTENT_TYPE, contentType);
            }
            if (body != null) {
                request.addBody(body);
            }

            if (metadata != null && !metadata.isEmpty()) {
                request.addHeader(Constants.METADATA, metadata);
            }

            HttpClient client = HttpClient.getHttpClient();
            return client.execute(request);
        } catch (Exception e) {
            LOG.error(ExceptionUtils.getFullStackTrace(e));
            throw new StormAdapterException("Exception in request / response from pds. TRACKING_ID = " + trackingId, e);
        }
    }

    public EventHubDiscoveryResponse getEventHubDiscoveryResponse(EventHubDiscoveryResponse origResponse,
            String nameSpace) throws StormAdapterException {
        LOG.info("getEventHubDiscoveryResponse started :: original EventHubDiscoveryResponse = " + origResponse);
        List<EventHubDiscoveryResponse> responseList = retrieveDiscoveryResponse();
        if (responseList == null || responseList.isEmpty()) {
            throw new StormAdapterException("unable to get event hub connection details via pds");
        }

        String origReg = origResponse.getRegion();
        String origName = origResponse.getName();

        LOG.info("fetching spoutName = " + origName + " & region = " + origReg + ", from returned responseList = "
                + responseList);

        for (EventHubDiscoveryResponse resp : responseList) {

            final ConnectionStringBuilder connectionStringBuilder = new ConnectionStringBuilder(resp.getConnection());
            String tmpNameSpace = connectionStringBuilder.getEndpoint().getHost()
                    .replaceAll(".servicebus.windows.net", "");

            if (!StringUtils.isEmpty(tmpNameSpace) && tmpNameSpace.equals(nameSpace) && !StringUtils.isEmpty(origReg)
                    && !StringUtils.isEmpty(origName) && origName.equals(resp.getName()) && origReg
                    .equals(resp.getRegion())) {
                return resp;
            }
        }
        return null;
    }
}