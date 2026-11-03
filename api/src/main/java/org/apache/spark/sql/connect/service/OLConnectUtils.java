package org.apache.spark.sql.connect.service;

import org.apache.spark.sql.SparkSession;

import java.util.Optional;

public class OLConnectUtils {
    public static Optional<SparkSession> fromClientEnd(SessionKey key){
        var sess = SparkConnectService$.MODULE$.sessionManager().getIsolatedSessionIfPresent(key);
        if (sess.isDefined()) {
            return Optional.of( sess.get().session() );
        } else {
            return Optional.empty();
        }
    }
}
