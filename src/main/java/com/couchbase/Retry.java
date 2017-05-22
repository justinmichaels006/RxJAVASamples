package com.couchbase;

import com.couchbase.client.java.Bucket;

public class Retry {

    private static final int ThreadCount = 10;

    public static void main(String[] args) {

        Bucket bucket = ConnectionManager.getConnection();
        
        try {
            ConcurrentQuery.testParalel(bucket);
        } catch (Exception e)
        {
            System.out.println(e);
        }

        // Disconnect and clear all allocated resources
        bucket.close();
    }
}