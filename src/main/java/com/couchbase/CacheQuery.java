package com.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.AbstractDocument;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.*;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import rx.Observable;

import java.util.List;

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

        String newSelect = "SELECT META(raas).id AS _ID, META(raas).cas AS _CAS FROM raas WHERE " +
                "(LOWER(description) LIKE LOWER(\"%%\")) " +
                "AND _class = \"com.aexp.wwcas.raas.web.service.api.usecase.UseCase\"";

        N1qlParams params = N1qlParams.build().adhoc(false).consistency(ScanConsistency.STATEMENT_PLUS);
        N1qlQuery newQuery = N1qlQuery.simple(newSelect, params);

        List<JsonDocument> docs = bucket.async().query(newQuery)
                .flatMap(AsyncN1qlQueryResult::rows)
                .map(row -> row.value().getString("_ID"))
                .flatMap(id -> bucket.async().get(id))
                .toList()
                .toBlocking()
                .single();

        result.subscribe(output -> System.out.println(output));

        return ("Look for document theResult");

    }
}
