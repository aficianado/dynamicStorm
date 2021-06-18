package com.chaoppo.db.storm.http;

import com.chaoppo.db.storm.error.StormAdapterException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Class defined basic HTTP request to send. Request contains the URL, body, http headers, method and TLS if used.
 *
 * @author PANDEAM
 */
public class Request {

    // constants
    public static final String COLON = ":";
    public static final String HTTPS = "https";
    public static final String HTTP = "http";
    public static final String FORWARD_SLASH = "/";

    boolean isTLS;
    private HttpURL url;
    private HttpMethod method;
    private byte[] body;
    private Map<String, String> headers = new HashMap<>();

    public Request() {
    }

    public Request(String url) throws StormAdapterException {
        this.url = new HttpURL();
        buildUrl(url.trim());
    }

    /**
     * Parse the URL supplied to request and extract the SSL information and hostname
     *
     * @param url requested url with host name and path
     * @throws Exception if url scheme is not valid
     */
    private void buildUrl(String url) throws StormAdapterException {
        int endPos = url.indexOf(COLON);
        int pos = endPos + 3;
        String scheme = url.substring(0, endPos);

        if (scheme == null || (!scheme.equals(HTTPS) && !scheme.equals(HTTP))) {
            throw new StormAdapterException("URL scheme not valid, scheme = " + scheme);
        }
        isTLS = scheme.equals(HTTPS);

        this.url.setUrlScheme(scheme);
        // host name
        endPos = url.indexOf(FORWARD_SLASH, pos + 1);
        String hostName = endPos == -1 ? url.substring(pos) : url.substring(pos, endPos);
        if (hostName != null) {
            int index = hostName.indexOf(COLON);
            this.url.setHostName(index != -1 ? hostName.substring(0, index) : hostName);
        }
        this.url.setUrl(url);
    }

    public byte[] body() {
        return body;
    }

    public String getUrl() {
        return url.toString();
    }

    public Request addHeader(String headerName, String value) {
        headers.put(headerName, value);
        return this;
    }

    public Set<Entry<String, String>> headers() {
        return Collections.unmodifiableSet(headers.entrySet());
    }

    public Request addBody(String body) {
        this.body = body.getBytes();
        return this;
    }

    public Request addMethod(HttpMethod method) {
        this.method = method;
        return this;
    }

    public HttpMethod method() {
        return this.method;
    }

    /**
     * Class defined the http url format
     */
    private static class HttpURL {

        private String urlScheme;
        private String hostName;
        private String url;

        public String getUrlScheme() {
            return urlScheme;
        }

        public void setUrlScheme(String urlScheme) {
            this.urlScheme = urlScheme;
        }

        public String getHostName() {
            return hostName;
        }

        public void setHostName(String hostName) {
            this.hostName = hostName;
        }

        @Override
        public String toString() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }
    }
}
