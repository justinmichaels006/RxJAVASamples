package com.couchbase;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.ReplicaMode;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.query.AsyncN1qlQueryRow;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Func1;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.java.util.retry.RetryBuilder.anyOf;

public class MetricQuery {

    public static List<String> getKeys(Bucket bucket) {

        N1qlParams params = N1qlParams.build().adhoc(false);
        //String thequery = "SELECT meta().id FROM `beer-sample` WHERE type =  \"brewery\"";
        String thequery = "SELECT meta().id FROM `beer-sample`";
        N1qlQuery simpleQ = N1qlQuery.simple(thequery, params);
        List<String> keyarray = new ArrayList<>();
        int i = 0;

        bucket.async()
                .query(simpleQ)
                .flatMap(result ->
                        result.errors()
                                .flatMap(e -> Observable.<AsyncN1qlQueryRow>error(new CouchbaseException("N1QL Error/Warning: " + e)))
                                .switchIfEmpty(result.rows())
                )
                .map(AsyncN1qlQueryRow::value)
                .toBlocking()
                .subscribe(
                        rowContent -> keyarray.add(rowContent.get("id").toString()),
                        runtimeError -> runtimeError.printStackTrace()
                );

        return keyarray;
    }

    public static List<JsonDocument> getMetricsFromFullDocs_1(Bucket bucket, List<String> somekeyarray) {
        List<JsonDocument> list = Observable.from(somekeyarray)
                .flatMap(theKey -> {
                    return bucket.async().get(theKey)
                            .retryWhen(anyOf(BackpressureException.class).max(10)
                                    .delay(Delay.fixed(10, TimeUnit.MILLISECONDS))
                                    .build())
                            .doOnError(System.err::println)
                            .onExceptionResumeNext(Observable.empty());
                }).toList().toBlocking().single();

        return list;
    }

    public static List<JsonDocument> getMetricsFromFullDocs_2(Bucket bucket, List<String> somekeyarray) {
        List<JsonDocument> list = Observable.from(somekeyarray)
                .flatMap(new Func1<String, Observable<JsonDocument>>() {
                    @Override
                    public Observable<JsonDocument> call(String id) {
                        return bucket.async().get(id)
                                .retryWhen(anyOf(BackpressureException.class).max(10)
                                        .delay(Delay.fixed(10, TimeUnit.MILLISECONDS))
                                        .build())
                                .doOnError(System.err::println)
                                .onErrorResumeNext(error -> {
                                    return bucket.async().getFromReplica(id, ReplicaMode.ALL);
                                })
                                .onExceptionResumeNext(Observable.empty());
                    }
                }).toList().toBlocking().single();

        return list;
    }

    public static List<JsonDocument> getMetricsFromFullDocs_3(Bucket bucket, List<String> somekeyarray) {
        CountDownLatch latch = new CountDownLatch(somekeyarray.size());
        List<JsonDocument> list = null;

        list = Observable.from(somekeyarray).flatMap(new Func1<String, Observable<JsonDocument>>() {
            @Override
            public Observable<JsonDocument> call(String docId) {
                return bucket.async().get(docId).doOnCompleted(Action0);

                //b.subscribe(new Subscriber<JsonDocument>() {
                @Override
                public void onCompleted () {
                    latch.countDown();
                }

                @Override
                public void onError (Throwable e){
                    e.printStackTrace(System.out);
                    latch.countDown();
                }

                @Override
                public void onNext (JsonDocument doc){
                    //list.add(doc);
                }
            }
        }).toList();

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return list;
    }
}
