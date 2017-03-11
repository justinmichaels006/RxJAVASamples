package com.couchbase;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.deps.io.netty.channel.ConnectTimeoutException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.couchbase.client.java.transcoder.JsonTranscoder;
import org.json.simple.parser.JSONParser;
import rx.Observable;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.java.util.retry.RetryBuilder.anyOf;

public class BulkLoader {

    public String bLoad(Bucket bucket) throws IOException, org.json.simple.parser.ParseException {

        int numDocs = 20000;
        long start = System.nanoTime();
        final int MAX_RETRIES = 20000;
        final int RETRY_DELAY = 50;
        final int MAX_DELAY = 1000;

        final String filePath1 = "/path/to/file.json";
        FileReader reader1 = null;

        try {
            reader1 = new FileReader(filePath1);
        } catch (FileNotFoundException ex) {
            //ex.printStackTrace();
            ex.getMessage();
        }

        JSONParser jsonParser = new JSONParser();
        JsonTranscoder trans = new JsonTranscoder();
        List<JsonDocument> docArray = new ArrayList<>();
        JsonObject jsonObj = null;
        JsonDocument jsonDoc = null;

        String jsonString1 = jsonParser.parse(reader1).toString();

        for (int i = 0; i < numDocs; i++) {

            try {
                jsonObj = trans.stringToJsonObject(jsonString1);
            } catch (Exception e) {e.printStackTrace();}

            // Create an id to use
            UUID theID = UUID.randomUUID();

            // Add the ID as an attribute to the document
            jsonObj.put("THIS_ID", i + "::" + theID.toString());
            // Add other logic here (TODO faker)
            // Create the json document
            jsonDoc = JsonDocument.create(theID.toString(), jsonObj);
            // Build the array of items to load
            // TODO Batching
            docArray.add(i, jsonDoc);
        }

        Observable
                .from(docArray)
                .flatMap(doc -> {
                    return bucket.async().upsert(doc)
                            // do retry for each op individually to not fail the full batch
                            .retryWhen(anyOf(BackpressureException.class)
                                    .max(MAX_RETRIES)
                                    .delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build())
                            .retryWhen(anyOf(TemporaryFailureException.class)
                                    .max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.MILLISECONDS, RETRY_DELAY, MAX_DELAY)).build())
                            .retryWhen(anyOf(ConnectTimeoutException.class)
                                    .max(MAX_RETRIES).delay(Delay.exponential(TimeUnit.SECONDS, RETRY_DELAY, MAX_DELAY)).build());
                }).toBlocking().subscribe(document1 -> {});

        long end = System.nanoTime();

        System.out.println("Bulk loading " + numDocs + " docs took: " + TimeUnit.NANOSECONDS.toSeconds(end - start) + "s.");
        return ("Bulk loading " + numDocs + " docs took: " + TimeUnit.NANOSECONDS.toSeconds(end - start) + "s.");
    }

}
