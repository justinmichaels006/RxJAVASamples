package com.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.AbstractDocument;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.AsyncN1qlQueryResult;
import com.couchbase.client.java.query.AsyncN1qlQueryRow;
import com.couchbase.client.java.query.N1qlQuery;
import rx.Observable;

public class CacheQuery {

    public String cQuery(Bucket bucket) {

        Observable<JsonObject> someQuery = bucket.async()
                .query(N1qlQuery.simple("SELECT * FROM testload WHERE META().id IS NOT MISSING"))
                .flatMap(AsyncN1qlQueryResult::rows)
                .map(AsyncN1qlQueryRow::value)
                .flatMap(value -> bucket.async().upsert(JsonDocument.create("theResult", 30, value))
                .map(AbstractDocument::content));

        Observable<JsonObject> result = bucket.async().get("theResult")
                .map(AbstractDocument::content)
                .switchIfEmpty(someQuery);

        result.subscribe(output -> System.out.println(output));

        return ("Look for document theResult");

    }
}
