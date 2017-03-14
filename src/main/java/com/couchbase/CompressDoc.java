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

    public String legacyCompress(Bucket b, Bucket b2) {

        LegacyTranscoder lTrans = new LegacyTranscoder(1);
        List<LegacyDocument> docArray = new ArrayList<>();
        final int MAX_RETRIES = 20000;
        final int RETRY_DELAY = 50;
        final int MAX_DELAY = 1000;
        int i = 0;

        ViewResult result = b.query(ViewQuery.from("beer", "allkeys"));
        //N1qlQueryResult result2 = myB.query((N1qlQuery.simple("select meta(`beer-sample`).id from `beer-sample`")));

        for (ViewRow row : result) {
        //for (N1qlQueryRow row : result2) {

            // Create an id to use
            UUID newID = UUID.randomUUID();

            JsonDocument jsonDoc = b.get(row.id());
            //JsonDocument jsonDoc = myB.get(row.toString());

            LegacyDocument ldoc = LegacyDocument.create(newID.toString(), jsonDoc);
            Object newdoc = lTrans.encode(ldoc);
            LegacyDocument ldoc2 = LegacyDocument.create(newID.toString(), newdoc.toString());

            b2.async().upsert(ldoc2);
            System.out.println(ldoc2.id());

            // Build the array of items to load (TODO Batching)
            //docArray.add(i, ldoc2);
            //i = i + 1;
        }

        return "compressed beer sample";

    }
}
