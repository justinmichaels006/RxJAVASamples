package com.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentQuery {
    private CouchbaseCluster cluster;
    private Bucket bucket;
    long totalTime = 0;
    int numResponses = 0;

    @Before
    public void setUp() throws InterruptedException {
        DefaultCouchbaseEnvironment environment = DefaultCouchbaseEnvironment
                .builder()
                .connectTimeout(30000)
                .queryTimeout(75000)
                .queryEndpoints(1)
                .kvTimeout(10000).build();
        List<String> nodes = Arrays.asList("192.168.61.101");
        cluster = CouchbaseCluster.create(environment, nodes);
        bucket = cluster.openBucket("beer-sample");

        environment.eventBus().get().subscribe(System.out::println);
    }

    @Test
    public void testParalel() throws InterruptedException {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT name, IFMISSINGORNULL(country,999), IFMISSINGORNULL(code,999) FROM `beer-sample` WHERE type = \"brewery\" AND name IS NOT MISSING LIMIT 1;");
//		builder.append("FROM APP_SPEC_DATA AS alerts ");
//		builder.append("WHERE  documentType = 'uCrew::AlertView' AND status = 'Open' AND area = 'InFlight' ");
//		builder.append("AND rootCategoryKey IN ['uCrew_AlertCategory_52b4c981-90bd-436e-8e42-415e9089dc3b','uCrew_AlertCategory_2b5d18a0-b988-4274-8b55-5951be76ec9e'] ");
//		builder.append("AND IFMISSING(delayReason,999) IN ['IL','IM','IS','IX','LTIL','LTIM','LTIS','LTIX']  ");
//		builder.append("AND ARRAY_LENGTH(flightLegs) > 0  ");
//		builder.append("ORDER BY alerts.flightLegDepartureTime, META().id DESC ");
//		builder.append("LIMIT 10 OFFSET 0;");

        System.out.println(builder.toString());

        /*Statement n1ql2 = Select.select("name", "IFMISSINGORNULL(country,999)", "IFMISSINGORNULL(code,999)")
                .from("beer-sample")
                .where(("type").equals("brewery").and("name")*/
        final N1qlQuery n1ql = N1qlQuery.simple(builder.toString(), N1qlParams
                .build().adhoc(false));

        List<N1qlQuery> n1qlArray = new ArrayList<>();

        //ExecutorService executor = Executors.newFixedThreadPool(10);
        //Set<Callable<N1qlQueryResult>> callables = new HashSet<Callable<N1qlQueryResult>>();
        final AtomicInteger tracker = new AtomicInteger(1);

        int q = 10;

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
                    }).toBlocking().subscribe(qresult -> System.out.println("Total execution this query: " + (System.currentTimeMillis() - totalTimeStart)));

        totalTime = System.currentTimeMillis() - totalTimeStart;
        System.out.println("Total execution time across all threads: " + totalTime + "ms");

        bucket.close();
        cluster.disconnect();
    }

    /*@After
    public void tearDown() {
        bucket.close();
        cluster.disconnect();
    }*/
}
