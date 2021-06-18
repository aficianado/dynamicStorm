package com.chaoppo.db.storm.storm;

import com.chaoppo.db.storm.error.StormAdapterException;
import com.chaoppo.db.storm.spout.StormAdapterFactory;
import com.chaoppo.db.storm.spout.StormDynaSpout;
import com.chaoppo.db.storm.util.Constants;
import org.apache.storm.testing.FeederSpout;
import org.apache.storm.tuple.Fields;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class StormAdapterFactoryTest {

    private static StormAdapterFactory stormDynaAdapter;
    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @BeforeClass
    public static void setupClass() throws Exception {
        long connExp = Instant.now().atOffset(ZoneOffset.UTC).toEpochSecond() + 1000;

        List<StormDynaSpout> mockSpouts = new ArrayList<>();

        StormDynaSpout scp = new StormDynaSpout(new FeederSpout(new Fields(Constants.BULK_EVENT_HUB_SPOUT_EMIT_FIELD)),
                "FeederSpout", "east", 1, connExp);
        mockSpouts.add(scp);

        stormDynaAdapter = new StormAdapterFactory("mock/com/chaoppo/db/connectivity/mock", "https://pdsUrl");
        stormDynaAdapter.withBulkMsgCount(1);
        stormDynaAdapter.withHttpProxyHost("httpProxyHost");
        stormDynaAdapter.withHttpProxyPort(3128);

        when(stormDynaAdapter.getDiscoveryClient().getStormDynaSpout(1)).thenReturn(mockSpouts);
    }

    @Test
    public void testGetstormDynaAdapterFactoryInstance() throws StormAdapterException {
        StormAdapterFactory stormDynaAdapterLocal = new StormAdapterFactory("com/chaoppo/db/connectivity/mock",
                "https://pdsUrl");
        assertEquals("the same instance needs to be returned", stormDynaAdapterLocal,
                stormDynaAdapterLocal.withBulkMsgCount(1));
        assertEquals("the same instance needs to be returned", stormDynaAdapterLocal,
                stormDynaAdapterLocal.withHttpProxyHost("httpProxyHost"));
        assertEquals("the same instance needs to be returned", stormDynaAdapterLocal,
                stormDynaAdapterLocal.withHttpProxyPort(3128));
    }

    @Test
    public void testGetEventHubSpout() throws StormAdapterException {
        List<StormDynaSpout> mockSpouts = stormDynaAdapter.getEventHubSpout();
        assertEquals("the spout count should be 1 ", 1, mockSpouts.size());
    }

    @Test
    public void testSubscritionKeyIsNull() throws StormAdapterException {
        ExpectedException.none();
        exceptionRule.expect(StormAdapterException.class);
        exceptionRule.expectMessage("eventHubSubscriptionKey cannot be empty");
        StormAdapterFactory stormDynaAdapterLocal = new StormAdapterFactory(null, null);
        stormDynaAdapterLocal.getEventHubSpout();
    }

    @Test
    public void testPDSUrlIsNull() throws StormAdapterException {
        ExpectedException.none();
        exceptionRule.expect(StormAdapterException.class);
        exceptionRule.expectMessage("partnerDiscoveryUrl cannot be empty");
        StormAdapterFactory stormDynaAdapterLocal = new StormAdapterFactory("com/chaoppo/db/connectivity/mock", null);
        stormDynaAdapterLocal.getEventHubSpout();
    }

}
