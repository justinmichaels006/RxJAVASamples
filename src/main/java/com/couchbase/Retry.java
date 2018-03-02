package com.couchbase;

import com.couchbase.client.java.Bucket;
import org.json.simple.parser.ParseException;

import java.io.IOException;

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

        //WF Start
        final String key="21st_amendment_brewery_cafe";
        try {
            byte[] response2 =  wf.getByteRecordNew(key, bucket2);

            if(null != response2)
            {
                System.out.println("found record in couchbase");
                System.out.println(response2);
            }
            else
            {
                System.out.println("did not find record in couchbase");
            }
            System.out.println("Done !!!!");
            System.exit(0);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        //WF End

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