package com.chaoppo.db.storm.http;

import com.chaoppo.db.storm.error.StormAdapterRuntimeException;
import org.apache.storm.shade.org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLConnection;
import java.security.cert.CertificateException;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

public class HttpClient {

    // constants: default properties used to create http client
    public static final int READ_TIMEOUT = 10;
    public static final int CONNECT_TIMEOUT = 10;
    public static final String TLS_VERSION = "TLSv1.2";

    private static final HttpClient INSTANCE = new HttpClient();
    private static final Logger LOG = LoggerFactory.getLogger(HttpClient.class);

    private SSLSocketFactory sslFactory;
    private HostnameVerifier hostNameVerifier;
    private int readTimeout;
    private int connectTimeout;

    private HttpClient() {
        try {
            // Create a trust manager that does not validate certificate chains
            final TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {

                @Override
                public void checkClientTrusted(java.security.cert.X509Certificate[] chain,
                        String authType) throws CertificateException {
                    LOG.info("checkClientTrusted is called which is not supported");
                }

                @Override
                public void checkServerTrusted(java.security.cert.X509Certificate[] chain,
                        String authType) throws CertificateException {
                    LOG.info("checkServerTrusted is called which is not supported");
                }

                @Override
                public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                    return new java.security.cert.X509Certificate[]{};
                }
            }};

            // Install the all-trusting trust manager
            final SSLContext sslContext = SSLContext.getInstance(TLS_VERSION);
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());

            // Create an ssl socket factory with our all-trusting manager
            final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
            this.setSocketFactory(sslSocketFactory);
            this.hostNameVerifier = (hostname, session) -> true;
            this.hostnameVerifier(hostNameVerifier);
            this.connectTimeout(CONNECT_TIMEOUT, TimeUnit.SECONDS);
            this.readTimeout(READ_TIMEOUT, TimeUnit.SECONDS);

        } catch (Exception e) {
            LOG.error("Exception while setting up the trust store " + ExceptionUtils.getFullStackTrace(e));
            throw new StormAdapterRuntimeException(e);
        }

    }

    private void setSocketFactory(SSLSocketFactory sslSocketFactory) {
        this.sslFactory = sslSocketFactory;
    }

    private void readTimeout(int time, TimeUnit seconds) {
        this.readTimeout = (int) seconds.toMillis(time);
    }

    private void connectTimeout(int time, TimeUnit seconds) {
        this.connectTimeout = (int) seconds.toMillis(time);
    }

    private void hostnameVerifier(HostnameVerifier hostnameVerifier) {
        this.hostNameVerifier = hostnameVerifier;
    }

    public static HttpClient getHttpClient() {
        return INSTANCE;
    }

    /**
     * Creates HTTP/HTTPS connection bases on request and send it requested URL in request.
     *
     * @param request Request send to host
     * @return response Returned response from server
     * @throws IOException In case of failed or interrupted connection.
     */
    public Response execute(Request request) throws IOException {
        LOG.info("Executing Request : " + request.method());
        Response response = null;
        HttpURLConnection httpConnection = getConnection(request);
        response = send(httpConnection, request);
        return response;
    }

    /**
     * Creates and open the Http connection, If SSL is present in url return HTTPS connection else HTTP connection
     *
     * @param request Request used to create HTTP/HTTPS connection.
     * @return connection new http connection object which will be used to send requests.
     * @throws IOException if an I/O exception occurs.
     */
    private HttpURLConnection getConnection(Request request) throws IOException {
        URL url = new URL(request.getUrl());
        HttpURLConnection httpConnection = null;
        httpConnection = (HttpURLConnection) url.openConnection();
        this.setHeaders(httpConnection, request);
        this.setMethod(httpConnection, request);
        httpConnection.setConnectTimeout(this.connectTimeout);
        httpConnection.setReadTimeout(this.readTimeout);
        if (request.isTLS) {
            ((HttpsURLConnection) httpConnection).setSSLSocketFactory(this.sslFactory);
        }
        return httpConnection;
    }

    /**
     * Sets requested HTTP methods in URLConnection
     *
     * @param connection HttpConnection used to communicate to server
     * @param req        Request object used send
     * @throws ProtocolException error at protocol level in connection
     */
    private void setMethod(URLConnection connection, Request req) throws ProtocolException {
        ((HttpURLConnection) connection).setRequestMethod(req.method().name());
    }

    /**
     * Set headers values in the HTTP connection
     *
     * @param connection HttpConnection used to communicate to server
     * @param req        Request object used send
     */
    private void setHeaders(URLConnection connection, Request req) {
        if (req.headers() != null) {
            req.headers().forEach(s -> connection.setRequestProperty(s.getKey(), s.getValue()));
        }
    }

    /**
     * Mehtod serves the purpose of sendingreceiving request/response from HTTP server. Based on the response code it
     * read the response from HTTP server. If body is present in request it write the request body to connection.
     *
     * @param connection HTTP connection used to send request
     * @param request    Request that is sent to request server.
     * @return response return from HTTP server
     * @throws IOException In case of failed or interrupted connection.
     */
    private Response send(HttpURLConnection connection, Request request) throws IOException {
        Response response = new Response();
        OutputStream os = null;
        InputStream is = null;

        try {
            // write data to open connection
            response.setSentTimeMilis(Instant.now().toEpochMilli());
            connection.setDoOutput(true);
            connection.connect();
            if (request.body() != null) {
                os = connection.getOutputStream();
                os.write(request.body());
                os.flush();
            }
            // get the response
            int code = connection.getResponseCode();
            if (HttpErrors.clientErrors.contains(code) || HttpErrors.serverErrors.contains(code)) {
                is = connection.getErrorStream();
            } else {
                is = connection.getInputStream();
            }
            byte[] buff = new byte[2048];
            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            int read = 0;
            if (is != null) {
                while ((read = is.read(buff)) != -1) {
                    byteArray.write(buff, 0, read);
                }
            }
            response.setCode(code);
            response.setMessage(connection.getResponseMessage());
            response.setResponseHeaders(connection.getHeaderFields());
            response.setBody(byteArray.toByteArray());
            response.setReceiveTimeMilis(Instant.now().toEpochMilli());
            return response;
        } finally {
            if (os != null) {
                os.close();
            }
            if (is != null) {
                is.close();
            }
        }
    }

}
