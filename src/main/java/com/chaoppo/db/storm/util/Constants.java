package com.chaoppo.db.storm.util;

public final class Constants {

    public static final String BULK_EVENT_HUB_SPOUT_EMIT_FIELD = "Msg";
    // HTTP headers
    public static final String OCP_APIM_SUB_KEY = "Ocp-Apim-Subscription-Key";
    public static final String TRACKING_ID = "TrackingId";
    public static final String OCP_APIM_TRACE = "Ocp-Apim-Trace";
    public static final String HOST = "Host";
    public static final String METADATA = "MetaData";
    public static final String CONTENT_TYPE = "Content-Type";
    public static final String USER_AGENT = "User-Agent";
    public static final String JSON = "application/json; charset=utf-8";
    public static final String OCTET_STREAM = "application/octet-stream";
    public static final String TIME_ELAPSED = "Time elapsed in executing Partner discovery {}";
    public static final String EXCEPTION_MSG = "Exception from the discovery service , response code is %s";
    public static final String STORM_DYNA_ADAPTER = "storm-dyna-adapter";
    public static final String OUTPUT_MSG = "message";
    public static final String OUTPUT_MSG_META_DATA = "metadata";

    private Constants() {

    }
}
