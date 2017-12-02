package com.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.List;

public class Retry {

    private static final int ThreadCount = 10;

    public static void main(String[] args) throws IOException, ParseException, InterruptedException {

        Bucket bucket = ConnectionManager.getConnection();
        Bucket bucket2 = ConnectionManager.getConnection2();

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

        //Wells Fargo Start
        long timeBefore = System.currentTimeMillis();
        JsonArray theKeys = MetricQuery.getKeys(bucket);
        long timeAfter = System.currentTimeMillis();
        System.out.println(timeAfter - timeBefore);
        System.out.println(theKeys.size());

        //DEBUG: System.out.println(theKeys);

        System.out.println("Got the keys now the docs");
        timeBefore = System.currentTimeMillis();
        List<JsonDocument> theDocs = RefDoc.qBulk(bucket, theKeys);
        timeAfter = System.currentTimeMillis();
        System.out.println(timeAfter - timeBefore);
        //Wells Fargo End

        //RH Start
        /*CountListQueue.CountCreate(bucket);
        System.out.println("done");
        JsonObject thecount = bucket.get("companycount").content();
        System.out.println(thecount);*/
        //RH End

/*        //AppD Start
        List<String> keyArray = MetricQuery.getKeys(bucket2);

            System.out.println("Bulk version 1... ");
            long timeBefore = System.currentTimeMillis();
            List<JsonDocument> docArray = MetricQuery.getMetricsFromFullDocs_1(bucket2, keyArray);
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
        //AppD End*/

        // Disconnect and clear all allocated resources
        System.out.println("debug ... bye");
        Boolean a = ConnectionManager.closeBucket(bucket);
        Boolean b = ConnectionManager.closeBucket(bucket2);
        System.out.println(a);
        //System.out.println(b);
    }
}