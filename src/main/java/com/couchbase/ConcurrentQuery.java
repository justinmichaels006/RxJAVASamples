package com.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.query.N1qlQuery;
import rx.Observable;

import java.util.ArrayList;
import java.util.List;

public class ConcurrentQuery {

    public static Long testParalel(Bucket bucket) throws InterruptedException {

        long totalTime = 0;
        List<N1qlQuery> n1qlArray = new ArrayList<>();

        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"A:1\",  {\"type\":\"A\",\"value\":\"FOX\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"A:2\",  {\"type\":\"A\",\"value\":\"COP\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"A:3\",  {\"type\":\"A\",\"value\":\"TAXI\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"A:4\",  {\"type\":\"A\",\"value\":\"LINCOLN\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"A:5\",  {\"type\":\"A\",\"value\":\"ARIZONA\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"A:6\",  {\"type\":\"A\",\"value\":\"WASHINGTON\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"A:7\",  {\"type\":\"A\",\"value\":\"DELL\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"A:10\",  {\"type\":\"A\",\"value\":\"LUCENT\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"B:1\",  {\"type\":\"A\",\"value\":\"TROT\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"B:2\",  {\"type\":\"A\",\"value\":\"CAR\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"B:3\",  {\"type\":\"A\",\"value\":\"CAB\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"B:6\",  {\"type\":\"A\",\"value\":\"MONUMENT\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"B:7\",  {\"type\":\"A\",\"value\":\"PC\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"B:8\",  {\"type\":\"A\",\"value\":\"MICROSOFT\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"B:9\",  {\"type\":\"A\",\"value\":\"APPLE\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"B:11\",  {\"type\":\"A\",\"value\":\"SCOTCH\"}) RETURNING *;"));
        System.out.println(n1qlArray.toString());

        /*Statement n1ql2 = Select.select("name", "IFMISSINGORNULL(country,999)", "IFMISSINGORNULL(code,999)")
                .from(i("beer-sample"))
                .where(x("type").eq(s("brewery"))
                        .and(x("name").isNotMissing()))
                .limit(1);
        final N1qlQuery n1ql = N1qlQuery.simple(n1ql2, N1qlParams.build().adhoc(false));
        int q = 20;
        for (int x = 0; x < q; x++) {
            n1qlArray.add(n1ql);
        }*/

        final long totalTimeStart = System.currentTimeMillis();

        Observable
                .from(n1qlArray)
                .flatMap(qry -> {
                    return bucket.async().query(qry);
                            //.retryWhen(anyOf(BackpressureException.class).max(5).delay(Delay.exponential(TimeUnit.MILLISECONDS, 1, 2)).build());
                            //.retryWhen(anyOf(OnErrorFailedException.class).max(2).delay(Delay.exponential(TimeUnit.MILLISECONDS, 1, 2)).build());
                    }).flatMap(nxt -> {
                        return nxt.info();
                        }).toBlocking().subscribe(qresult -> System.out.println(qresult.elapsedTime()));

        totalTime = System.currentTimeMillis() - totalTimeStart;
        System.out.println("Total execution time across all threads: " + totalTime + "ms");
        return totalTime;
    }
}

