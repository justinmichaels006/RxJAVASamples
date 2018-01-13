package com.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.LegacyDocument;

public class LegacyExample {

    public static String createLegacyDoc(Bucket bucket) {

        LegacyDocument newDoc = LegacyDocument.create("key", "content");

        // Open bucket on the old SDK
        /*CouchbaseClient client = new CouchbaseClient(
                Arrays.asList(URI.create("http://127.0.0.1:8091/pools")),
                "default",
                ""
        );*/

        // Create document on old SDK
        //client.set("fromOld", "Hello from Old!").get();

        // Create document on new SDK
        bucket.upsert(LegacyDocument.create("fromNew", "Hello from New!"));

        // Read old from new
        System.out.println(bucket.get("fromOld", LegacyDocument.class));

        // Read new from old
        //System.out.println(client.get("fromNew"));

        return "id";
    }

}
