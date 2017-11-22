package com.couchbase;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.JsonLongDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.couchbase.client.java.transcoder.JsonTranscoder;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.java.util.retry.RetryBuilder.anyOf;

public class CountListQueue {

    public static void CountCreate(Bucket bucket) {

        int MAX_RETRIES = 10;
        int MAX_DELAY = 10;
        int RETRY_DELAY = 10;

        BufferedReader buff1 = null;
        FileReader reader1 = null;
        JsonTranscoder trans = new JsonTranscoder();
        JsonObject jsonObj;
        JsonDocument jsonDoc = null;
        String jsonKey = null;
        final String filePath1 = "/Users/justin/Documents/Demo/data/companies.json";

        try {
            reader1 = new FileReader(filePath1);
            buff1 = new BufferedReader(reader1);
            String line = buff1.readLine();
            while (line != null) {
                jsonObj = trans.stringToJsonObject(line);
                jsonKey = jsonObj.get("name").toString();
                //  System.out.println(jsonKey);
                jsonDoc = jsonDoc.create(jsonKey, jsonObj);

                //doWork(jsonDoc, bucket);
                Observable
                        .just(jsonDoc)
                        .flatMap(thedoc -> {
                            return bucket.async().upsert(thedoc)
                                    .retryWhen(anyOf(BackpressureException.class).max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, MAX_DELAY, RETRY_DELAY)).build())
                                    .retryWhen(anyOf(TemporaryFailureException.class).max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, MAX_DELAY, RETRY_DELAY)).build())
                                    .doOnError(new Action1<Throwable>() {
                                        @Override
                                        public void call(Throwable e) {
                                            e.printStackTrace(System.out);
                                        }
                                    });
                        })
                        .flatMap(new Func1<JsonDocument, Observable<JsonLongDocument>>() {
                            @Override
                            public Observable<JsonLongDocument> call(JsonDocument jsonDocument) {
                                return bucket.async().counter("companycount", 1, 1);
                            }
                        }).toBlocking().subscribe();
                line = buff1.readLine();
            }
        } catch(Exception e) {
            System.out.println("DEBUG 42");
            e.printStackTrace();
        }
    };
}
