package com.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Retry {

    private static final int ThreadCount = 10;

    public static void main(String[] args) {
        //public BinaryDocument saveBinary (String bucketName, BinaryDocument document, Long expiry, TimeUnit timeUnit){
        //ConnectionManager connectionManager = ConnectionManager.getConnection();

        Bucket bucket = ConnectionManager.getConnection2();
        JsonDocument adoc = null;
        JsonObject aobj = null;

        ExecutorService executioner = Executors.newFixedThreadPool(ThreadCount);

            for (int i=0; i<10; i++) {
                try {
                    adoc.create(String.valueOf(i), aobj.put("count", i));
                    Runnable thework = new SomeRunnable(adoc, bucket);
                    executioner.execute(thework);
                } catch (Exception e) {
                    System.out.println("Exception: " + e.toString());
                }
            }

    }

    public static class SomeRunnable implements Runnable {
        private final JsonDocument adoc;
        private final Bucket ab;
        int expiry = 30;
        int retryCount = 0;
        int timer = 1;

        SomeRunnable(JsonDocument adoc, Bucket bucket){
            this.adoc = adoc;
            ab = bucket;
        }

        @Override
        public void run() {

            try {
                ab.upsert(adoc, expiry, TimeUnit.SECONDS);
            } catch (Exception e) {
                if (retryCount >= 2 * ConnectionManager.MAX_RETRIES) {
                    e.printStackTrace(System.out);
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
