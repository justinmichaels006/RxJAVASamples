package com.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;

import java.util.concurrent.TimeUnit;

public class Retry {

    public static void main(String[] args) {
        //public BinaryDocument saveBinary (String bucketName, BinaryDocument document, Long expiry, TimeUnit timeUnit){
        //ConnectionManager connectionManager = ConnectionManager.getConnection();

        Bucket bucket = ConnectionManager.getConnection();
        JsonDocument document = null;

        int retryCount = 1;
        int timer = 1;
        int expiry = 30;

            for (int i=0; i<10000; i++) {
                try {
                    bucket.upsert(document, expiry, TimeUnit.SECONDS);
                } catch (Exception e) {
                    if (retryCount >= 2 * ConnectionManager.MAX_RETRIES) {
                        e.printStackTrace(System.out);
                        break;
                    }

                    try {
                        Thread.sleep(timer * 250);
                    } catch (InterruptedException interException) {
                        interException.printStackTrace();
                    }

                    retryCount++;
                }
            }

    }
}
