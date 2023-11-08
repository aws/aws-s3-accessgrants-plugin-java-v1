package com.amazonaws.s3accessgrants.cache;

import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static com.amazonaws.s3accessgrants.cache.S3AccessGrantsTestConstants.TEST_S3_BUCKET;
import static com.amazonaws.s3accessgrants.cache.S3AccessGrantsTestConstants.TEST_S3_PREFIX;
import static com.amazonaws.s3accessgrants.cache.internal.S3AccessGrantsCacheUtils.getBucketName;

public class S3AccessGrantsUtilTest {
    @Test
    public void getBucketName_from_s3Prefix() {
        // When
        String bucketName = getBucketName(TEST_S3_PREFIX);
        // Then
        assertThat(bucketName).isEqualTo(TEST_S3_BUCKET);
    }
}
