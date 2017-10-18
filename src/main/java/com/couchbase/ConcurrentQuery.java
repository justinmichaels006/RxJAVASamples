package com.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.Select;
import com.couchbase.client.java.query.Statement;
import rx.Observable;

import java.util.ArrayList;
import java.util.List;

import static com.couchbase.client.java.query.dsl.Expression.*;

public class ConcurrentQuery {

    public static Long testParalel(Bucket bucket) throws InterruptedException {

        long totalTime = 0;
        List<N1qlQuery> n1qlArray = new ArrayList<>();

        /*n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"A:1\",  {\"type\":\"A\",\"val\":\"FOX\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"A:2\",  {\"type\":\"A\",\"val\":\"COP\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"A:3\",  {\"type\":\"A\",\"val\":\"TAXI\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"A:4\",  {\"type\":\"A\",\"val\":\"LINCOLN\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"A:5\",  {\"type\":\"A\",\"val\":\"ARIZONA\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"A:6\",  {\"type\":\"A\",\"val\":\"WASHINGTON\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"A:7\",  {\"type\":\"A\",\"val\":\"DELL\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"A:10\",  {\"type\":\"A\",\"val\":\"LUCENT\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"B:1\",  {\"type\":\"A\",\"val\":\"TROT\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"B:2\",  {\"type\":\"A\",\"val\":\"CAR\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"B:3\",  {\"type\":\"A\",\"val\":\"CAB\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"B:6\",  {\"type\":\"A\",\"val\":\"MONUMENT\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"B:7\",  {\"type\":\"A\",\"val\":\"PC\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"B:8\",  {\"type\":\"A\",\"val\":\"MICROSOFT\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"B:9\",  {\"type\":\"A\",\"val\":\"APPLE\"}) RETURNING *;"));
        n1qlArray.add(N1qlQuery.simple("INSERT INTO testload (KEY, VALUE) VALUES ( \"B:11\",  {\"type\":\"A\",\"val\":\"SCOTCH\"}) RETURNING *;"));
        System.out.println(n1qlArray.toString());*/

        JsonDocument someMutation = JsonDocument.create("key", JsonObject.create().put("somevalue", "thevalue"));
        JsonDocument docWithToken = bucket.upsert(someMutation);

        Statement n1ql2 = Select.select("header")
                .from(i("testload"))
                .where(x("data.version").eq(s("1.0")));
        final N1qlQuery n1ql = N1qlQuery.simple(n1ql2, N1qlParams.build().adhoc(false).consistentWith(docWithToken));
        int q = 20;
        for (int x = 0; x < q; x++) {
            n1qlArray.add(n1ql);

        }

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

