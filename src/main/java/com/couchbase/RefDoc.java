package com.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.AsyncN1qlQueryResult;
import com.couchbase.client.java.query.AsyncN1qlQueryRow;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.transcoder.JsonTranscoder;
import rx.Observable;

import java.util.ArrayList;
import java.util.List;

public class RefDoc {

    public static JsonObject addRefDoc(Bucket bucket) {

        //Create website REFDOC for a brewery based on a beer
        //Wihtout defining String abrewery = "21st_amendment_brewery_cafe";
        String abeer = "somebeer";
        JsonDocument jdoc = null;
        JsonObject jobj = null;
        JsonTranscoder trans = new JsonTranscoder();

        Observable<String> jREFDOC = bucket.async().get(abeer)
                .map(doc -> doc.content().getString("brewery_id"))
                .flatMap(id -> bucket.async().get(id))
                .map(doc3 -> {
                    return doc3.content().getString("website");
                });

        jdoc = bucket.get(abeer);
        System.out.println("Got doc " + jdoc.content().getString("brewery_id"));

        jobj = jdoc.content();

        try {
            JsonObject finalJobj = jobj;
            jREFDOC.toBlocking()
                    .subscribe(theSite -> bucket.upsert(JsonDocument.create(theSite.toString(), finalJobj)));
        } catch (Exception e) {
            System.out.println("Error during web doc " + e);
        }
        return jobj;
    }

    public static List<JsonDocument> qBulk(Bucket bucket, JsonArray theList) {

        List<JsonDocument> reportDocs = new ArrayList<JsonDocument>();
        //List<String> theSublist = new ArrayList<String>();

        //Syncronous
        /*for (int i = 0; i < reportsDocs.size(); i++) {
            //Get the document based on key array
            JsonDocument somedoc = bucket.get(reportsDocs.getString(i));
            theList.add(somedoc);
            //Get the attributes needed based on key array
            //theSublist.add(bucket.lookupIn(reportsDocs.getString(i)).get("email").execute().toString());
        }*/

        //Async
        Observable.from(theList)
                .flatMap(a -> { return bucket.async().get(a.toString());})
                .toBlocking()
        .subscribe(
                result -> reportDocs.add(result)
        );

        System.out.println(reportDocs);
        System.out.println(theList);
        //System.out.println("theSublist");
        //System.out.println(theSublist);
        return reportDocs;
    }

    public static String qTest(Bucket bucket) {

        String matrixQ = "select top.reporting, top.email as supEml, IFMISSINGORNULL(ARRAY_COUNT(top.reporting), 0) as directReportsCount\n" +
                "from testload top\n" +
                "where top.cliCode = 2033 and  top.jobTitle = 'ABSENCE RADAR TOP NODE'";

        final N1qlQuery n1ql5 = N1qlQuery.simple(matrixQ, N1qlParams.build().adhoc(false));

        AsyncN1qlQueryRow qDocs = bucket.async()
                .query(n1ql5)
                .flatMap(AsyncN1qlQueryResult::rows)
                .toBlocking().single();

        System.out.println("theQ");
        System.out.println(qDocs);
        return "ok";
    }
}
