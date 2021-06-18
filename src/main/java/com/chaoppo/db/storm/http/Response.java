package com.chaoppo.db.storm.http;

import com.nimbusds.jose.util.StandardCharset;

import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Class defines basic HTTP response, It contains response body, code, status, and response hesaders
 */
public class Response {

    private int code;
    private String status;
    private String message;
    private long sentTimeMilis;
    private long receiveTimeMilis;
    private byte[] body;
    private Map<String, List<String>> responseHeaders;

    public boolean isSuccessful() {
        return (code == HttpURLConnection.HTTP_ACCEPTED || code == HttpURLConnection.HTTP_CREATED
                || code == HttpURLConnection.HTTP_OK) ? true : false;
    }

    public String body() {
        return new String(body, StandardCharset.UTF_8);
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Map<String, List<String>> getResponseHeaders() {
        return Collections.unmodifiableMap(responseHeaders);
    }

    public void setResponseHeaders(Map<String, List<String>> responseHeaders) {
        this.responseHeaders = responseHeaders;
    }

    public void setSentTimeMilis(long sentTimeMilis) {
        this.sentTimeMilis = sentTimeMilis;
    }

    public void setReceiveTimeMilis(long receiveTimeMilis) {
        this.receiveTimeMilis = receiveTimeMilis;
    }

    public long receivedResponseAtMillis() {
        return this.receiveTimeMilis;
    }

    public long sentRequestAtMillis() {
        return this.sentTimeMilis;
    }
}
