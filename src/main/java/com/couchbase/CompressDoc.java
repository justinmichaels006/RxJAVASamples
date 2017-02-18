package com.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.LegacyDocument;
import com.couchbase.client.java.transcoder.LegacyTranscoder;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class CompressDoc {

    public static void main(String[] args) {

        Bucket myB = ConnectionManager.getConnection();
        Bucket myB2 = ConnectionManager.getConnection2();

        LegacyTranscoder lTrans = new LegacyTranscoder(1);
        List<LegacyDocument> docArray = new ArrayList<>();
        final int MAX_RETRIES = 20000;
        final int RETRY_DELAY = 50;
        final int MAX_DELAY = 1000;
        int i = 0;
        //String abeer = "21st_amendment_brewery_cafe-21a_ipa";

        ViewResult result = myB.query(ViewQuery.from("beer", "allkeys"));
        //N1qlQueryResult result2 = myB.query((N1qlQuery.simple("select meta(`beer-sample`).id from `beer-sample`")));

        for (ViewRow row : result) {
        //for (N1qlQueryRow row : result2) {

            // Create an id to use
            UUID newID = UUID.randomUUID();

            //JsonDocument jsonDoc = myB.get(row.toString());
            JsonDocument jsonDoc = myB.get(row.id());

            LegacyDocument ldoc = LegacyDocument.create(newID.toString(), jsonDoc);
            Object newdoc = lTrans.encode(ldoc);
            LegacyDocument ldoc2 = LegacyDocument.create(newID.toString(), newdoc.toString());

            // Add other logic here (TODO faker)
            myB2.upsert(ldoc2);

            // Build the array of items to load (TODO Batching)
            /*docArray.add(i, ldoc2);
            i = i + 1;*/
        }

        /*Observable
                .from(docArray)
                .flatMap(doc -> {
                    return myB2.async().upsert(doc)
                            // do retry for each op individually to not fail the full batch
                            .retryWhen(anyOf(BackpressureException.class)
                                    .max(MAX_RETRIES)
                                    .delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build())
                            .retryWhen(anyOf(TemporaryFailureException.class)
                                    .max(MAX_RETRIES)
                                    .delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build());
                }).toBlocking().subscribe(document1 -> {});*/

        System.out.println("compressed beer sample");
        myB.close();
        myB2.close();

    }
}
