package com.couchbase;

import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import java.util.Arrays;
import java.util.List;

public class ConnectionManager {

    public static final int MAX_RETRIES = 3;
    public static final String bucketName = "test"; // more elegant use of bucket name
    public static final String bucketPassword = "";
    public static final List<String> nodes = Arrays.asList("192.168.61.101"); //dnssrv entry here

    public static Bucket getConnection() {
        CouchbaseEnvironment environment = DefaultCouchbaseEnvironment.builder()
                .dnsSrvEnabled(false)
                .kvEndpoints(2) //if you have batch upload can gain throughput
                // but with small operations can cause contention with socket overhead
                .computationPoolSize(4) // very rare needed to be changed
                //.queryEndpoints(2) // long running N1QL queries
                //.observeIntervalDelay()
                .retryStrategy(FailFastRetryStrategy.INSTANCE) // only needed in demanding cache-only use case
                .build();
        CouchbaseCluster cluster = CouchbaseCluster.create(environment, nodes);
        final Bucket bucket = cluster.openBucket(bucketName);
        return bucket;
    }
}
