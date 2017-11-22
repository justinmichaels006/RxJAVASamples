package com.couchbase;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.ReplicaMode;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.query.AsyncN1qlQueryRow;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import rx.Observable;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.java.util.retry.RetryBuilder.anyOf;

public class MetricQuery {

    public static JsonArray getKeys(Bucket bucket) {

        N1qlParams params = N1qlParams.build().adhoc(false);
        //String thequery = "SELECT meta().id FROM `beer-sample` WHERE type =  \"brewery\"";
        //String thequery = "SELECT meta().id FROM `beer-sample`";
        String matrixQ = "select top.reporting, top.email as supEml, IFMISSINGORNULL(ARRAY_COUNT(top.reporting), 0) as directReportsCount\n" +
                "from testload top\n" +
                "where top.cliCode = 2033 and  top.jobTitle = 'ABSENCE RADAR TOP NODE'";
        N1qlQuery simpleQ = N1qlQuery.simple(matrixQ, params);
        JsonArray keyarray = JsonArray.create();

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
                        rowContent -> keyarray.add(rowContent.getArray("reporting")),
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
        List<JsonDocument> list = new ArrayList<JsonDocument>();
        //DEBUG: System.out.println(somekeyarray.size());

        Observable.from(somekeyarray)
                .flatMap(new Func1<String, Observable<JsonDocument>>() {
                    @Override
                    public Observable<JsonDocument> call(String docId) {
                        return bucket.async().get(docId)
                                .doOnError(new Action1<Throwable>() {
                                    @Override
                                    public void call(Throwable e) {
                                        System.out.println("DEBUG: OnError from inside 3");
                                        latch.countDown();
                                        e.printStackTrace(System.out);
                                    }
                                })
                                .doOnCompleted(new Action0() {
                                    @Override
                                    public void call() {

                                    }
                                })
                                .doOnNext(new Action1<JsonDocument>() {
                                    @Override
                                    public void call(JsonDocument jd) {
                                        latch.countDown();
                                    }
                                });
                    }
                }).doOnError(new Action1<Throwable>() {
                    @Override
                    public void call(Throwable e) {
                        System.out.println("DEBUG: OnError from 3");
                        e.printStackTrace(System.out);
                    }
                }).subscribe(a -> list.add(a));

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return list;
    }
}
