package com.couchbase;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.deps.io.netty.channel.ConnectTimeoutException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.couchbase.client.java.transcoder.JsonTranscoder;
import org.json.simple.parser.JSONParser;
import rx.Observable;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.java.util.retry.RetryBuilder.anyOf;

/**
 * Created by justin on 11/9/16.
 */
public class BulkLoader {

    public static void main(String[] args) throws IOException, org.json.simple.parser.ParseException {

        CouchbaseEnvironment environment = DefaultCouchbaseEnvironment.builder()
                .dnsSrvEnabled(false)
                .kvEndpoints(2) //if you have batch upload can gain throughput
                // but with small operations can cause contention with socket overhead
                .computationPoolSize(4) // very rare needed to be changed
                //.queryEndpoints(2) // long running N1QL queries
                //.observeIntervalDelay()
                .retryStrategy(FailFastRetryStrategy.INSTANCE) // only needed in demanding cache-only use case
                .build();
        CouchbaseCluster cluster = CouchbaseCluster.create(environment, "192.168.61.101");
        final Bucket bucket = cluster.openBucket("test");

        int numDocs = 20000;
        long start = System.nanoTime();
        final int MAX_RETRIES = 20000;
        final int RETRY_DELAY = 50;
        final int MAX_DELAY = 1000;

        final String filePath1 = "/Users/justin/Documents/javastuff/SampleLoad/ACCT.json";
        FileReader reader1 = null;

        try {
            reader1 = new FileReader(filePath1);
        } catch (FileNotFoundException ex) {
            //ex.printStackTrace();
            ex.getMessage();
        }

        JSONParser jsonParser = new JSONParser();
        JsonTranscoder trans = new JsonTranscoder();
        List<JsonDocument> docArray = new ArrayList<>();
        JsonObject jsonObj = null;
        JsonDocument jsonDoc = null;

        String jsonString1 = jsonParser.parse(reader1).toString();

        try {
            jsonObj = trans.stringToJsonObject(jsonString1);
        } catch (Exception e) {e.printStackTrace();}

        for (int i = 0; i < numDocs; i++) {

            // Create an id to use
            UUID theID = UUID.randomUUID();
            AnInsert sd = new AnInsert(theID.toString(), jsonObj, bucket);

            //jsonObj.put("THIS_ID", i + "::" + eventID.toString());
            //jsonDoc = JsonDocument.create(eventID.toString(), jsonObj);
            //Observable.just(jsonDoc).map(bucket.async().upsert(jsonDoc));
            //docArray.add(i, jsonDoc);
        }

        /*Observable
                .from(docArray)
                .flatMap(doc -> {
                    return bucket.async().upsert(doc)
                            // do retry for each op individually to not fail the full batch
                            .retryWhen(anyOf(BackpressureException.class)
                                    .max(MAX_RETRIES)
                                    .delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build())
                            .retryWhen(anyOf(TemporaryFailureException.class)
                                    .max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build())
                            .retryWhen(anyOf(ConnectTimeoutException.class)
                                    .max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.SECONDS, RETRY_DELAY, MAX_DELAY)).build());
                }).toBlocking().subscribe(document1 -> {});*/

        long end = System.nanoTime();

        System.out.println("Bulk loading " + numDocs + " docs took: " + TimeUnit.NANOSECONDS.toSeconds(end - start) + "s.");
        bucket.close();
        cluster.disconnect();
    }

    public static class AnInsert {

        //private String key;
        //private JsonObject content;
        //private Bucket bucket;
        private int MAX_RETRIES = 10;
        private int RETRY_DELAY = 1;
        private int MAX_DELAY = 5;

        public AnInsert(String key, JsonObject content, Bucket bucket) {

            Observable<JsonDocument> theProcessor = Observable
                    .just(key)
                    .map(s -> JsonDocument.create(s, content))
                    .flatMap(doc -> bucket.async().upsert(doc)
                            .retryWhen(anyOf(BackpressureException.class)
                                    .max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build())
                            .retryWhen(anyOf(TemporaryFailureException.class)
                                    .max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build())
                            .retryWhen(anyOf(ConnectTimeoutException.class)
                                    .max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.SECONDS, RETRY_DELAY, MAX_DELAY)).build()));

            try {
                theProcessor.toBlocking()
                        .subscribe(jsonDocument -> System.out.println("Inserted " + jsonDocument));
            } catch (Exception e) {
                System.out.println("Error during bulk insert " + e);
            }
        }
    }
}
