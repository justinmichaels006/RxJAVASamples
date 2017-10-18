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

public class Retry {

    private static final int ThreadCount = 10;

    public static void main(String[] args) throws IOException, ParseException, InterruptedException {

        Bucket bucket = ConnectionManager.getConnection();
        Bucket bucket2 = ConnectionManager.getConnection2();

        int numDocs = 10;
        int groupDocs = 10;
        FileReader reader1 = null;
        JSONParser jsonParser = new JSONParser();
        JsonTranscoder trans = new JsonTranscoder();
        List<String> keyArray = new ArrayList<>();
        List<JsonDocument> docArray = new ArrayList<>();
        JsonObject jsonObj = null;
        JsonDocument jsonDoc = null;
        Random r = new Random();
        final String filePath1 = "/Users/justin/Documents/javastuff/RxJAVASamples/src/main/java/com/couchbase/file.json";
        //final String filePath1 = "/Users/justin/Documents/javastuff/data.json";
        //BufferedReader buff1 = null;

        reader1 = new FileReader(filePath1);
        String jsonString1 =  jsonParser.parse(reader1).toString();

        try {
            jsonObj = trans.stringToJsonObject(jsonString1);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // This is a work in progress
        /*for (int i = 0; i < (numDocs*groupDocs); i=i+groupDocs) {
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

        }*/

        System.out.println("Starting... ");

        //Create multiple threads across different workloads
        /*try{
            SampleSub.SubDocModification(bucket, "test");
        } catch (Exception e) {
            e.printStackTrace();
        }*/
        //ConcurrentQuery.testParalel(bucket);
        //BulkLoader.bLoad(bucket, docArray);
        //RefDoc.addRefDoc(bucket2);
        //RefDoc.qBulk(bucket2);

        //AppD Start
        keyArray = MetricQuery.getKeys(bucket2);
        System.out.println("Bulk version 1... ");
        long timeBefore = System.currentTimeMillis();
        docArray = MetricQuery.getMetricsFromFullDocs_1(bucket2, keyArray);
        long timeAfter = System.currentTimeMillis();
        System.out.println(timeAfter - timeBefore);
        System.out.println(docArray.size());
        System.out.println("Bulk version 2... ");
        timeBefore = System.currentTimeMillis();
        docArray = MetricQuery.getMetricsFromFullDocs_2(bucket2, keyArray);
        timeAfter = System.currentTimeMillis();
        System.out.println(timeAfter - timeBefore);
        System.out.println(docArray.size());
        System.out.println("Bulk version 3... ");
        timeBefore = System.currentTimeMillis();
        docArray = MetricQuery.getMetricsFromFullDocs_3(bucket2, keyArray);
        timeAfter = System.currentTimeMillis();
        System.out.println(timeAfter - timeBefore);
        System.out.println(docArray.size());
        //AppD End

        // Disconnect and clear all allocated resources
        System.out.println("debug ... bye");
        Boolean a = ConnectionManager.closeBucket(bucket);
        Boolean b = ConnectionManager.closeBucket(bucket2);
        System.out.println(a);
        System.out.println(b);
    }
}