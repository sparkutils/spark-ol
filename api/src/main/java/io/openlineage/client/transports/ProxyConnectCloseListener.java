package io.openlineage.client.transports;

import com.google.common.collect.MapMaker;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.job.Naming;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connect.service.OLConnectUtils;
import org.apache.spark.sql.connect.service.SessionKey;
import org.apache.spark.sql.connect.service.SparkListenerConnectSessionClosed;
import org.apache.spark.sql.connect.service.SparkListenerConnectSessionStarted;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Calls emit all on the proxy
 */
public class ProxyConnectCloseListener extends SparkListener {

    // force the SparkSession to stay around
    private Map<SessionKey, SparkSession> sessionCache =
            new MapMaker().concurrencyLevel(
                Integer.parseInt(System.getenv().
                    getOrDefault("OPENLINEAGE__PROXY_CONCURRENCY_LEVEL", "4"))
                ).makeMap();

    @Override
    public void onOtherEvent(SparkListenerEvent event) {
        if (event instanceof SparkListenerConnectSessionStarted) {
            var ev = (SparkListenerConnectSessionStarted) event;
            var key = new SessionKey(ev.userId(), ev.sessionId());
            OLConnectUtils.fromClientEnd(key).ifPresent( sess -> {
                sessionCache.putIfAbsent(key, sess);
            });
        } else
        if (event instanceof SparkListenerConnectSessionClosed) {
            var ev = (SparkListenerConnectSessionClosed) event;
            var key = new SessionKey(ev.userId(), ev.sessionId());

            var sess = sessionCache.remove(key);
            if (sess != null) {
               SparkSession oldSess = SparkSession.getActiveSession().getOrElse( () -> null );
               try {
                   SparkSession.setActiveSession(sess);
                   ProxyTransportRemote.emitAll();
               } finally {
                   SparkSession.setActiveSession(oldSess);
               }
            }
        }
    }
}
