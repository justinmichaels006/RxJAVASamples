package com.couchbase;


import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;
import com.couchbase.client.core.message.kv.subdoc.multi.Mutation;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.subdoc.DocumentFragment;
import com.couchbase.client.java.subdoc.MutateInBuilder;

public class SampleSub {

    public static String SubDocModification(Bucket bucket, String aKey) throws Exception {

        System.out.println("Lookup attempt");

        DocumentFragment<Lookup> lookupRslt = bucket
                .lookupIn("Int02")
                .get("parent")
                .execute();


        JsonDocument somedoc = JsonDocument.create(aKey,
                JsonObject.create()
                        .put("logins", 0)
                        .put("searches", 0)
                        .put("path", 50)
        );
        bucket.upsert(somedoc);

        bucket.mutateIn(aKey)
                .counter("logins", 1,false)
                .counter("searches", 5, false)
                .counter("path", -1, false)
                .execute();

        String subValue = lookupRslt.content("parent", String.class);
        System.out.println("Lookup result" + subValue);

        System.out.println("Mutate attempt");
        
        MutateInBuilder builder = bucket.mutateIn(aKey);
        DocumentFragment<Mutation> result = builder.replace("parent", 40).execute();

        DocumentFragment<Lookup> result2 = bucket
                .lookupIn("Int02")
                .get("parent")
                .execute();
        System.out.println("Mutation result" + result2);

        return result.toString();

    }
}
