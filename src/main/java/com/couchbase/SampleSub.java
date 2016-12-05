package com.couchbase;


import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;
import com.couchbase.client.core.message.kv.subdoc.multi.Mutation;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.subdoc.DocumentFragment;
import com.couchbase.client.java.subdoc.MutateInBuilder;

public class SampleSub {

    public static void main(String[] args) throws Exception {

        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
                .dnsSrvEnabled(false)
                .kvEndpoints(4) //if you have batch upload can gain throughput
                // but with small operations can cause contention with socket overhead
                .computationPoolSize(4) // very rare needed to be changed
                .queryEndpoints(4) // long running N1QL queries
                //.observeIntervalDelay()
                .retryStrategy(FailFastRetryStrategy.INSTANCE) // only needed in demanding cache-only use case
                .build();
        CouchbaseCluster cluster = CouchbaseCluster.create(env, "192.168.61.101");
        Bucket bucket = cluster.openBucket("testload");

        System.out.println("Lookup attempt");

        DocumentFragment<Lookup> lookupRslt = bucket
                .lookupIn("Int02")
                .get("parent.name")
                .execute();

        String subValue = lookupRslt.content("parent.name", String.class);
        System.out.println("Lookup result" + subValue);

        System.out.println("Mutate attempt");

        MutateInBuilder builder = bucket.mutateIn("Int02");
        DocumentFragment<Mutation> result = builder.replace("parent.age", 40).execute();

        DocumentFragment<Lookup> result2 = bucket
                .lookupIn("Int02")
                .get("parent.age")
                .execute();
        System.out.println("Mutation result" + result2);

        bucket.close();
        cluster.disconnect();
    }
}
