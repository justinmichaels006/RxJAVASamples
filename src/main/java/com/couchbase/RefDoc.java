package com.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.transcoder.JsonTranscoder;
import rx.Observable;

import java.util.UUID;

/**
 * Created by justin on 1/20/17.
 */
public class RefDoc {

    public static void main(String[] args) {
        //public BinaryDocument saveBinary (String bucketName, BinaryDocument document, Long expiry, TimeUnit timeUnit){
        //ConnectionManager connectionManager = ConnectionManager.getConnection();

        Bucket bucket = ConnectionManager.getConnection();
        UUID newID = UUID.randomUUID();
        String abeer = "21st_amendment_brewery_cafe-21a_ipa";
        String abrew = "21st_amendment_brewery_cafe";
        JsonDocument jdoc = null;
        JsonObject jobj = null;
        //String jREFDOC = null;
        JsonTranscoder trans = new JsonTranscoder();

        //JsonDocument theBrewery = null;

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
        /*Observable<JsonDocument> siteREFDOC = Observable
                .just(jREFDOC)
                .map(s -> JsonDocument.create(s, finalJobj))
                .flatMap(doc -> bucket.async().upsert(doc));*/

        try {
            jREFDOC.toBlocking()
                    .subscribe(theSite -> JsonDocument.create(theSite.toString(), finalJobj));
                    //.flatMap(doc -> bucket.async().upsert(doc));
                            //System.out.println("Web REFDOC " + theSite));
        } catch (Exception e) {
            System.out.println("Error during web doc " + e);
        }

        bucket.close();
    }
}
