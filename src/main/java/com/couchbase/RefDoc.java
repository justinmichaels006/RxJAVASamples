package com.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.transcoder.JsonTranscoder;
import rx.Observable;

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
                .map(doc3 -> {return doc3.content().getString("website");})
                ;

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
}
