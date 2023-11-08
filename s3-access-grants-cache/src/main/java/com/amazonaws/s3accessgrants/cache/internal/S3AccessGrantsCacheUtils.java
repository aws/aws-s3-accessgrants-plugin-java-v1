package com.amazonaws.s3accessgrants.cache.internal;

import java.net.URI;

public class S3AccessGrantsCacheUtils {
    public static String getBucketName(String s3Prefix) {
        return URI.create(s3Prefix).getHost();
    }

}
