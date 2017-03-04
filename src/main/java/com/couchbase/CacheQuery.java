package com.couchbase;

import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.AbstractDocument;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.AsyncN1qlQueryResult;
import com.couchbase.client.java.query.AsyncN1qlQueryRow;
import com.couchbase.client.java.query.N1qlQuery;
import rx.Observable;

public class CacheQuery {

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

        Observable<JsonObject> someQuery = bucket.async()
                .query(N1qlQuery.simple("SELECT * FROM testload WHERE META().id IS NOT MISSING"))
                .flatMap(AsyncN1qlQueryResult::rows)
                .map(AsyncN1qlQueryRow::value)
                .flatMap(value -> bucket.async().upsert(JsonDocument.create("theResult", 30, value))
                .map(AbstractDocument::content));

        Observable<JsonObject> result = bucket.async().get("theResult")
                .map(AbstractDocument::content)
                .switchIfEmpty(someQuery);

        result.subscribe(output -> System.out.println(output));

    }
}
