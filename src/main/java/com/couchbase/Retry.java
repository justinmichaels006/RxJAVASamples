package com.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;

import java.util.concurrent.TimeUnit;

/**
 * Created by justin on 12/5/16.
 */
public class Retry {

    public static void main(String[] args) {
        //public BinaryDocument saveBinary (String bucketName, BinaryDocument document, Long expiry, TimeUnit timeUnit){
        //ConnectionManager connectionManager = ConnectionManager.getConnection();

        Bucket bucket = ConnectionManager.getConnection();
        JsonDocument document = null;

        int retryCount = 1;
        int timer = 1;
        int expiry = 30;

            while (true) {
                try {
                    //return bucket.upsert(document, expiry, timeUnit);
                    bucket.upsert(document, expiry, TimeUnit.SECONDS);
                } catch (Exception e) {
                    if (retryCount >= 2 * ConnectionManager.MAX_RETRIES) {
                    /*GenericExtendedActions.audit(AppInfo.getAppInfo().getOsContext(),
                            "Error while inserting binary document using upsert. Current retry count:" + retryCount
                                    + "-" + e.getMessage(),
                            "CouchbaseIntegration");*/
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
