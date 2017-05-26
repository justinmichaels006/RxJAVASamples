package com.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.transcoder.JsonTranscoder;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class Retry {

    private static final int ThreadCount = 10;

    public static void main(String[] args) throws IOException, ParseException {

        Bucket bucket = ConnectionManager.getConnection();
        Bucket bucket2 = ConnectionManager.getConnection2();

        int numDocs = 10;
        int groupDocs = 10;
        FileReader reader1 = null;
        JSONParser jsonParser = new JSONParser();
        JsonTranscoder trans = new JsonTranscoder();
        List<JsonDocument> docArray = new ArrayList<>();
        JsonObject jsonObj = null;
        JsonDocument jsonDoc = null;
        Random r = new Random();
        final String filePath1 = "/Users/justin/Documents/javastuff/RxJAVASamples/src/main/java/com/couchbase/file.json";
        //BufferedReader buff1 = null;

        reader1 = new FileReader(filePath1);
        String jsonString1 =  jsonParser.parse(reader1).toString();

        try {
            jsonObj = trans.stringToJsonObject(jsonString1);
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (int i = 0; i < (numDocs*groupDocs); i=i+groupDocs) {
            for (int z = 0; z < groupDocs; z++) {
                jsonObj.put("eqr_xref", i);
                // Create an id to use
                UUID uid = UUID.fromString("38400000-8cf0-11bd-b23e-10b96e4ef00d");
                String theID = uid.randomUUID().toString();

                // Add the ID as an attribute to the document
                jsonObj.put("reward_id", theID);
                // Create the json document
                jsonDoc = JsonDocument.create(theID, jsonObj);
                System.out.println("reward_id..." + jsonObj.get("reward_id"));
                System.out.println("eqr_xref..." + jsonObj.get("eqr_xref"));
                System.out.println(jsonDoc);
                // Add other logic here (TODO faker)
                // TODO Batching
                //bucket.upsert(jsonDoc);
                docArray.add(i+z, jsonDoc);
            }

        }

        System.out.println("Starting... ");
        //ConcurrentQuery.testParalel(bucket);
        //BulkLoader.bLoad(bucket, docArray);
        RefDoc.addRefDoc(bucket2);

        // Disconnect and clear all allocated resources
        System.out.println("debug ... bye");
        bucket.close();
        bucket2.close();
    }
}