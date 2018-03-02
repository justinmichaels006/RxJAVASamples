package com.couchbase;

import com.couchbase.client.java.Bucket;

import javax.xml.bind.DatatypeConverter;

public class wf {

        public static byte[] getByteRecordNew(String key, Bucket client){

            final String recordByKey = client.get(key).content().toString();

            byte[] bytes = recordByKey.getBytes();

            String base64Encoded = DatatypeConverter.printBase64Binary(bytes);
            System.out.println("Encoded Json:\n");
            System.out.println(base64Encoded + "\n");

            byte[] base64Decoded = DatatypeConverter.parseBase64Binary(base64Encoded);
            System.out.println("Decoded Json:\n");
            System.out.println(new String(base64Decoded));

            return bytes;
        }
}
