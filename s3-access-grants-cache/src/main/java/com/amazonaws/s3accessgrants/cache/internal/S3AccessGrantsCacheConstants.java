package com.amazonaws.s3accessgrants.cache.internal;

public class S3AccessGrantsCacheConstants {
    public static final int DEFAULT_ACCOUNT_ID_MAX_CACHE_SIZE = 1_000;
    public static final int MAX_LIMIT_ACCOUNT_ID_MAX_CACHE_SIZE = 1_000_000;
    public static final int DEFAULT_ACCOUNT_ID_EXPIRE_CACHE_AFTER_WRITE_SECONDS = 86_400; // one day
    public static final int MAX_LIMIT_ACCOUNT_ID_EXPIRE_CACHE_AFTER_WRITE_SECONDS = 2_592_000; // 30 days

    public static final int DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE = 30_000;
    public static final int MAX_LIMIT_ACCESS_GRANTS_MAX_CACHE_SIZE = 1_000_000;
    public static final int CACHE_EXPIRATION_TIME_PERCENTAGE = 90;

    public static final int ACCESS_DENIED_CACHE_SIZE = 3_000;
}
