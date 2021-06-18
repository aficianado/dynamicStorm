package com.chaoppo.db.storm.http;

import javax.net.ssl.HttpsURLConnection;
import java.net.HttpURLConnection;
import java.util.HashSet;
import java.util.Set;

/**
 * Class defines HTTP error from HTTP server/client used by HttpClient to parse the response
 */
public final class HttpErrors {

    protected static final Set<Integer> clientErrors = new HashSet<>();
    protected static final Set<Integer> serverErrors = new HashSet<>();

    static {
        // adding clients errors
        clientErrors.add(HttpURLConnection.HTTP_BAD_REQUEST);
        clientErrors.add(HttpURLConnection.HTTP_UNAUTHORIZED);
        clientErrors.add(HttpURLConnection.HTTP_PAYMENT_REQUIRED);
        clientErrors.add(HttpURLConnection.HTTP_FORBIDDEN);
        clientErrors.add(HttpURLConnection.HTTP_NOT_FOUND);
        clientErrors.add(HttpURLConnection.HTTP_BAD_METHOD);
        clientErrors.add(HttpURLConnection.HTTP_NOT_ACCEPTABLE);
        clientErrors.add(HttpURLConnection.HTTP_PROXY_AUTH);
        clientErrors.add(HttpURLConnection.HTTP_CLIENT_TIMEOUT);
        clientErrors.add(HttpURLConnection.HTTP_CONFLICT);
        clientErrors.add(HttpURLConnection.HTTP_GONE);
        clientErrors.add(HttpURLConnection.HTTP_LENGTH_REQUIRED);
        clientErrors.add(HttpURLConnection.HTTP_PRECON_FAILED);
        clientErrors.add(HttpURLConnection.HTTP_ENTITY_TOO_LARGE);
        clientErrors.add(HttpURLConnection.HTTP_REQ_TOO_LONG);
        clientErrors.add(HttpURLConnection.HTTP_UNSUPPORTED_TYPE);
        // server errors
        serverErrors.add(HttpsURLConnection.HTTP_INTERNAL_ERROR);
        serverErrors.add(HttpsURLConnection.HTTP_NOT_IMPLEMENTED);
        serverErrors.add(HttpsURLConnection.HTTP_BAD_GATEWAY);
        serverErrors.add(HttpsURLConnection.HTTP_UNAVAILABLE);
        serverErrors.add(HttpsURLConnection.HTTP_GATEWAY_TIMEOUT);
        serverErrors.add(HttpsURLConnection.HTTP_VERSION);

    }

    private HttpErrors() {
    }
}
