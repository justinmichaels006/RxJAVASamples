package com.couchbase;

import com.couchbase.client.core.metrics.DefaultLatencyMetricsCollectorConfig;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConnectionManager {

    public static final String bucketName = "testload"; // more elegant use of bucket name
    public static final String bucketName2 = "beer-sample";
    public static final String bucketPassword = "password";
    public static final List<String> nodes = Arrays.asList("192.168.61.101"); //dnssrv entry here

    private static ConnectionManager instance;
    private ConnectionManager() {
    }

    public static CouchbaseEnvironment environment = DefaultCouchbaseEnvironment.builder()
            .dnsSrvEnabled(false) //Set to true in production
            .ioPoolSize(2)           // threadpool for IO
            .computationPoolSize(2)  // threadpool for non-io tasks like serialization. very rare needed to be changed
            .callbacksOnIoPool(true) // run callbacks on IO thread
            .kvEndpoints(2) //if you have batch upload can gain throughput
            //.callbacksOnIoPool(true)
            // but with small operations can cause contention with socket overhead
            //.queryEndpoints(2) // long running N1QL queries
            //.jHiccupEnabled(false)   // jHiccup runs a thread; good for diagnosis but remove for sizing tests
            //.observeIntervalDelay() //replicateTo internal
            .retryStrategy(FailFastRetryStrategy.INSTANCE) // only needed in demanding cache-only use case
            .networkLatencyMetricsCollectorConfig(DefaultLatencyMetricsCollectorConfig.create(1, TimeUnit.MINUTES))
            .networkLatencyMetricsCollectorConfig(DefaultLatencyMetricsCollectorConfig.builder()
                            .targetUnit(TimeUnit.MILLISECONDS).build())
            .mutationTokensEnabled(true)
            .build();

    public static CouchbaseCluster cluster = CouchbaseCluster.create(environment, nodes);
            //.authenticate("user", "pass");

    public synchronized static Bucket getConnection() {
        final Bucket bucket = cluster.openBucket(bucketName, bucketPassword);
        return bucket;
    }

    public synchronized static Bucket getConnection2() {
        //System.setProperty(N1qlQueryExecutor.ENCODED_PLAN_ENABLED_PROPERTY, "false");
        final Bucket bucket = cluster.openBucket(bucketName2, bucketPassword);
        return bucket;
    }

    public synchronized static Boolean closeBucket(Bucket b) {
        Boolean result = b.close();
        return result;
    }

    public static rx.Subscription getBus() {
        //More information here:
        //https://developer.couchbase.com/documentation/server/current/sdk/java/collecting-information-and-logging.html
        Logger logger = Logger.getLogger("com.couchbase.client");
        logger.setLevel(Level.FINE);
        for(Handler h : logger.getParent().getHandlers()) {
            if(h instanceof ConsoleHandler){
                h.setLevel(Level.INFO); //Use .FINE for debug level
            }
        }
        return environment.eventBus().get().subscribe(System.out::println);
    }

}
