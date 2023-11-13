package com.amazonaws.s3accessgrants.internal;

import com.amazonaws.thirdparty.apache.logging.Log;
import com.amazonaws.thirdparty.apache.logging.LogFactory;
import com.amazonaws.util.ValidationUtils;

public class S3AccessGrantsUtils {
    public static final Boolean DEFAULT_CACHE_SETTING = true;
    public static final int DEFAULT_DURATION = 3600;
    private static final Log logger = LogFactory.getLog(S3AccessGrantsUtils.class);

    public static void argumentNotNull(Object param, String message) {
        try{
            ValidationUtils.assertNotNull(param, message);
        } catch (NullPointerException e) {
            logger.error(message);
            throw new IllegalArgumentException(message);
        }
    }
}
