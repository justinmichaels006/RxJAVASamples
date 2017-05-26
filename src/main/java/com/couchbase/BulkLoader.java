package com.couchbase;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.error.TemporaryFailureException;
import rx.Observable;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.java.util.retry.RetryBuilder.anyOf;

public class BulkLoader {

    public static String bLoad(Bucket bucket, List<JsonDocument> docArray) {

        final int MAX_RETRIES = 20000;
        final int RETRY_DELAY = 50;
        final int MAX_DELAY = 1000;

        long start = System.nanoTime();
        Observable
                .from(docArray)
                .flatMap(doc -> {
                    return bucket.async().upsert(doc)
                            // do retry for each op individually to not fail the full batch
                            .retryWhen(anyOf(BackpressureException.class)
                                    .max(MAX_RETRIES)
                                    .delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build())
                            .retryWhen(anyOf(TemporaryFailureException.class)
                                    .max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build());
                }).toBlocking().subscribe(document1 -> {});
        long end = System.nanoTime();

        System.out.println("Bulk loading " + docArray.size() + " docs took: " + TimeUnit.NANOSECONDS.toSeconds(end - start) + "s.");
        return ("Bulk loading " + docArray.size()  + " docs took: " + TimeUnit.NANOSECONDS.toSeconds(end - start) + "s.");
    }

}
