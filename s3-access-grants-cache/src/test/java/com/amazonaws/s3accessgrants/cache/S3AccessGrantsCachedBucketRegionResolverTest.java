/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.s3accessgrants.cache;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.HeadBucketResult;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.ArgumentMatchers.any;

public class S3AccessGrantsCachedBucketRegionResolverTest {
    private AmazonS3 s3Client = mock(AmazonS3.class);
    private S3AccessGrantsCachedBucketRegionResolver s3AccessGrantsCachedBucketRegionResolver;
    private String TEST_BUCKET_NAME;

    @Before
    public void setUp() {
        TEST_BUCKET_NAME = "test-bucket";
        s3AccessGrantsCachedBucketRegionResolver = S3AccessGrantsCachedBucketRegionResolver.builder().build();
        HeadBucketResult headBucketResponse = new HeadBucketResult().withBucketRegion(Regions.US_EAST_1.getName());
        when(s3Client.headBucket(any(HeadBucketRequest.class))).thenReturn(headBucketResponse);
    }

    @Test
    public void call_resolve_should_cache_the_bucket_region() {
        assertThat(s3AccessGrantsCachedBucketRegionResolver.resolve(s3Client, TEST_BUCKET_NAME)).isEqualTo(Regions.US_EAST_1);
        // initial request should be made to the service
        verify(s3Client, times(1)).headBucket(any(HeadBucketRequest.class));
        assertThat(s3AccessGrantsCachedBucketRegionResolver.resolve(s3Client, TEST_BUCKET_NAME)).isEqualTo(Regions.US_EAST_1);
        // No call should be made to the service as the region is already cached
        verify(s3Client, times(1)).headBucket(any(HeadBucketRequest.class));
    }

    @Test
    public void call_resolve_should_not_cache_the_bucket_region() {
        S3AccessGrantsCachedBucketRegionResolver localS3AccessGrantsCachedBucketRegionResolver = S3AccessGrantsCachedBucketRegionResolver.builder().build();
        HeadBucketResult headBucketResponse = new HeadBucketResult().withBucketRegion(null);
        when(s3Client.headBucket(any(HeadBucketRequest.class))).thenReturn(headBucketResponse);
        Assertions.assertThatThrownBy(() -> localS3AccessGrantsCachedBucketRegionResolver.resolve(s3Client, TEST_BUCKET_NAME)).isInstanceOf(AmazonServiceException.class);
        verify(s3Client, times(1)).headBucket(any(HeadBucketRequest.class));
        // since bucket region is null, cache will not store the entry
        Assertions.assertThatThrownBy(() -> localS3AccessGrantsCachedBucketRegionResolver.resolve(s3Client, TEST_BUCKET_NAME)).isInstanceOf(AmazonServiceException.class);
        verify(s3Client, times(2)).headBucket(any(HeadBucketRequest.class));
    }

    @Test
    public void verify_bucket_region_cache_expiration() throws InterruptedException {

        S3AccessGrantsCachedBucketRegionResolver localCachedBucketRegionResolver = S3AccessGrantsCachedBucketRegionResolver
                .builder()
                .expireCacheAfterWriteSeconds(1)
                .build();

        Assert.assertEquals(Regions.US_EAST_1, localCachedBucketRegionResolver.resolve(s3Client, TEST_BUCKET_NAME));
        verify(s3Client, times(1)).headBucket(any(HeadBucketRequest.class));
        Thread.sleep(2000);
        // should evict the entry and the subsequent request should call the service
        Assert.assertEquals(Regions.US_EAST_1, localCachedBucketRegionResolver.resolve(s3Client, TEST_BUCKET_NAME));
        verify(s3Client, times(2)).headBucket(any(HeadBucketRequest.class));

    }

    @Test
    public void call_bucket_region_cache_with_non_existent_bucket() {

        S3AccessGrantsCachedBucketRegionResolver localS3AccessGrantsCachedBucketRegionResolver = S3AccessGrantsCachedBucketRegionResolver.builder().build();
        when(s3Client.headBucket(any(HeadBucketRequest.class))).thenThrow(new AmazonS3Exception("Bucket does not exist"));
        Assertions.assertThatThrownBy(() ->  localS3AccessGrantsCachedBucketRegionResolver.resolve(s3Client, TEST_BUCKET_NAME)).isInstanceOf(AmazonServiceException.class);
        verify(s3Client, times(1)).headBucket(any(HeadBucketRequest.class));

    }

    @Test
    public void call_bucket_region_cache_resolve_returns_redirect() {

        AmazonS3Exception s3Exception = mock(AmazonS3Exception.class);
        S3AccessGrantsCachedBucketRegionResolver localS3AccessGrantsCachedBucketRegionResolver = S3AccessGrantsCachedBucketRegionResolver.builder().build();
        Map<String, String> regionList = new HashMap<>();
        regionList.put("x-amz-bucket-region", "us-east-1");
        when(s3Exception.getAdditionalDetails()).thenReturn(regionList);
        when(s3Client.headBucket(any(HeadBucketRequest.class))).thenThrow(s3Exception);
        Assert.assertEquals(Regions.US_EAST_1, localS3AccessGrantsCachedBucketRegionResolver.resolve(s3Client, TEST_BUCKET_NAME));
        verify(s3Client, times(1)).headBucket(any(HeadBucketRequest.class));

    }

    @Test
    public void call_bucket_region_cache_resolve_returns_redirect_with_null_region() {

        AmazonS3Exception s3Exception = mock(AmazonS3Exception.class);
        S3AccessGrantsCachedBucketRegionResolver localS3AccessGrantsCachedBucketRegionResolver = S3AccessGrantsCachedBucketRegionResolver.builder().build();
        Map<String, String> regionList = new HashMap<>();
        regionList.put("x-amz-bucket-region", null);
        when(s3Exception.getAdditionalDetails()).thenReturn(regionList);
        when(s3Client.headBucket(any(HeadBucketRequest.class))).thenThrow(s3Exception);
        Assertions.assertThatThrownBy(() -> localS3AccessGrantsCachedBucketRegionResolver.resolve(s3Client, TEST_BUCKET_NAME))
                .isInstanceOf(AmazonServiceException.class);

    }

    @Test
    public void call_bucket_region_cache_resolve_returns_non_redirect_with_region() {

        AmazonS3Exception s3Exception = mock(AmazonS3Exception.class);
        S3AccessGrantsCachedBucketRegionResolver localS3AccessGrantsCachedBucketRegionResolver = S3AccessGrantsCachedBucketRegionResolver.builder().build();
        Map<String, String> regionList = new HashMap<>();
        regionList.put("x-amz-bucket-region", "us-east-1");
        when(s3Exception.getAdditionalDetails()).thenReturn(regionList);
        when(s3Client.headBucket(any(HeadBucketRequest.class))).thenThrow(s3Exception);
        Assert.assertEquals(Regions.US_EAST_1, localS3AccessGrantsCachedBucketRegionResolver.resolve(s3Client, TEST_BUCKET_NAME));
        verify(s3Client, times(1)).headBucket(any(HeadBucketRequest.class));


    }

}
