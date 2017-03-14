package com.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.transcoder.JsonTranscoder;
import rx.Observable;

public class RefDoc {

    public JsonObject addRefDoc(Bucket bucket) {

        String abeer = "21st_amendment_brewery_cafe-21a_ipa";
        String abrew = "21st_amendment_brewery_cafe";
        JsonDocument jdoc = null;
        JsonObject jobj = null;
        JsonTranscoder trans = new JsonTranscoder();

        Observable theBrewery = bucket.async().get(abeer)
                .map(doc -> doc.content().getString("brewery_id"))
                .flatMap(id -> bucket.async().get(id));

        Observable jREFDOC = bucket.async().get(abrew)
                .map(doc3 -> {return doc3.content().getString("website");});

        try {
            theBrewery.toBlocking()
                    .subscribe(jsonDocument1 -> System.out.println("Brewery " + jsonDocument1));
        } catch (Exception e) {
            System.out.println("Error during insert " + e);
        }

        try {
            jobj = trans.stringToJsonObject(abrew);
        } catch (Exception e) {e.printStackTrace();}

        JsonObject finalJobj = jobj;

        try {
            jREFDOC.toBlocking()
                    .subscribe(theSite -> JsonDocument.create(theSite.toString(), finalJobj));
        } catch (Exception e) {
            System.out.println("Error during web doc " + e);
        }

        return finalJobj;
    }
}
