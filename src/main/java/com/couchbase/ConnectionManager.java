package com.couchbase;

import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import java.util.Arrays;
import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConnectionManager {

    public static final int MAX_RETRIES = 3;
    public static final String bucketName = "testload"; // more elegant use of bucket name
    public static final String bucketName2 = "travel-sample";
    public static final String bucketPassword = "";
    public static final List<String> nodes = Arrays.asList("192.168.61.101"); //dnssrv entry here

    public static CouchbaseEnvironment environment = DefaultCouchbaseEnvironment.builder()
            .dnsSrvEnabled(false)
            .kvEndpoints(2) //if you have batch upload can gain throughput
            // but with small operations can cause contention with socket overhead
            .computationPoolSize(4) // very rare needed to be changed
            //.queryEndpoints(2) // long running N1QL queries
            //.observeIntervalDelay()
            .retryStrategy(FailFastRetryStrategy.INSTANCE) // only needed in demanding cache-only use case
            .build();

    public static CouchbaseCluster cluster = CouchbaseCluster.create(environment, nodes);
            //.authenticate("user", "pass");

    public static Bucket getConnection() {
            final Bucket bucket = cluster.openBucket(bucketName, bucketPassword);
        return bucket;
    }

    public static Bucket getConnection2() {
        final Bucket bucket = cluster.openBucket(bucketName2, bucketPassword);
        return bucket;
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
