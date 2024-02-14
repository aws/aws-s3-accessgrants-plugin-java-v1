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
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.time.Duration;

import static com.amazonaws.s3accessgrants.cache.internal.S3AccessGrantsCacheConstants.BUCKET_REGION_CACHE_SIZE;
import static com.amazonaws.s3accessgrants.cache.internal.S3AccessGrantsCacheConstants.BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS;
import static com.amazonaws.s3accessgrants.cache.internal.S3AccessGrantsCacheConstants.MAX_BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS;
import static com.amazonaws.s3accessgrants.cache.internal.S3AccessGrantsCacheConstants.MAX_BUCKET_REGION_CACHE_SIZE;

public class S3AccessGrantsCachedBucketRegionResolver {

    private Cache<String, Regions> cache;
    private int maxCacheSize;
    private int expireCacheAfterWriteSeconds;
    private static final Log logger = LogFactory.getLog(S3AccessGrantsCachedBucketRegionResolver.class);

    public int getMaxCacheSize() {
        return maxCacheSize;
    }

    public int getExpireCacheAfterWriteSeconds() {
        return expireCacheAfterWriteSeconds;
    }

    public int expireCacheAfterWriteSeconds() {
        return expireCacheAfterWriteSeconds;
    }

    public int maxCacheSize() {
        return maxCacheSize;
    }

    protected CacheStats getCacheStats() { return cache.stats(); }

    public S3AccessGrantsCachedBucketRegionResolver.Builder toBuilder() {
        return new S3AccessGrantsCachedBucketRegionResolver.BuilderImpl(this);
    }

    public static S3AccessGrantsCachedBucketRegionResolver.Builder builder() {
        return new S3AccessGrantsCachedBucketRegionResolver.BuilderImpl();
    }

    private S3AccessGrantsCachedBucketRegionResolver() {
        this.maxCacheSize = BUCKET_REGION_CACHE_SIZE;
        this.expireCacheAfterWriteSeconds = BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS;
    }

    public Regions resolve(AmazonS3 s3Client, String bucket) throws AmazonS3Exception{
        Regions bucketRegion = cache.getIfPresent(bucket);
        if(bucketRegion == null) {
            logger.debug("bucket region not available in cache, fetching the region from the service!");
            if (s3Client == null) {
                throw new IllegalArgumentException("S3Client is required for the bucket region resolver!");
            }
            bucketRegion = resolveFromService(s3Client, bucket);
            if(bucketRegion != null) {
                cache.put(bucket, bucketRegion);
            }
        } else {
            logger.debug("bucket region available in cache!");
        }
        return bucketRegion;

    }

    private Regions resolveFromService(AmazonS3 s3Client, String bucket) {
        String resolvedRegion;
        try {
            logger.info("Making a call to S3 for determining the bucket region.");
            HeadBucketRequest bucketLocationRequest = new HeadBucketRequest(bucket);
            HeadBucketResult headBucketResponse = s3Client.headBucket(bucketLocationRequest);
            resolvedRegion = headBucketResponse.getBucketRegion();
        } catch (AmazonS3Exception e) {
            logger.debug("An exception occurred while make head bucket request to fetch bucket region. Attempting to extract the region from headers.");
            if (e.getAdditionalDetails() != null && e.getAdditionalDetails().get("x-amz-bucket-region") != null ) {
                // A fallback in case the head bucket requests fails.
                resolvedRegion = e.getAdditionalDetails().get("x-amz-bucket-region");
            } else {
                throw new AmazonServiceException(e.getMessage());
            }
        }
        if(resolvedRegion == null) throw new AmazonServiceException("S3 error. region cannot be determined for the specified bucket.");
        return Regions.fromName(resolvedRegion);
    }

    public interface Builder {
        S3AccessGrantsCachedBucketRegionResolver build();
        S3AccessGrantsCachedBucketRegionResolver.Builder maxCacheSize(int maxCacheSize);
        S3AccessGrantsCachedBucketRegionResolver.Builder expireCacheAfterWriteSeconds(int expireCacheAfterWriteSeconds);
    }

    static final class BuilderImpl implements S3AccessGrantsCachedBucketRegionResolver.Builder {
        private int maxCacheSize = BUCKET_REGION_CACHE_SIZE;
        private int expireCacheAfterWriteSeconds = BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS;

        private BuilderImpl() {
        }

        public BuilderImpl(S3AccessGrantsCachedBucketRegionResolver s3AccessGrantsCachedBucketRegionResolver) {
            maxCacheSize(s3AccessGrantsCachedBucketRegionResolver.maxCacheSize);
            expireCacheAfterWriteSeconds(s3AccessGrantsCachedBucketRegionResolver.expireCacheAfterWriteSeconds);
        }

        public int maxCacheSize() {
            return maxCacheSize;
        }

        public int expireCacheAfterWriteSeconds() {
            return expireCacheAfterWriteSeconds;
        }

        @Override
        public S3AccessGrantsCachedBucketRegionResolver.Builder maxCacheSize(int maxCacheSize) {
            if (maxCacheSize <= 0 || maxCacheSize > MAX_BUCKET_REGION_CACHE_SIZE) {
                throw new IllegalArgumentException(String.format("maxCacheSize needs to be in range (0, %d]",
                        MAX_BUCKET_REGION_CACHE_SIZE));
            }
            this.maxCacheSize = maxCacheSize;
            return this;
        }

        @Override
        public S3AccessGrantsCachedBucketRegionResolver.Builder expireCacheAfterWriteSeconds(int expireCacheAfterWriteSeconds) {
            if (expireCacheAfterWriteSeconds <= 0 || expireCacheAfterWriteSeconds > MAX_BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS) {
                throw new IllegalArgumentException(String.format("expireCacheAfterWriteSeconds needs to be in range (0, %d]",
                        MAX_BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS));
            }
            this.expireCacheAfterWriteSeconds = expireCacheAfterWriteSeconds;
            return this;
        }

        @Override
        public S3AccessGrantsCachedBucketRegionResolver build() {
            S3AccessGrantsCachedBucketRegionResolver resolver = new S3AccessGrantsCachedBucketRegionResolver();
            resolver.maxCacheSize = maxCacheSize();
            resolver.expireCacheAfterWriteSeconds = expireCacheAfterWriteSeconds();
            resolver.cache = Caffeine.newBuilder()
                    .maximumSize(maxCacheSize)
                    .expireAfterWrite(Duration.ofSeconds(expireCacheAfterWriteSeconds))
                    .build();
            return resolver;
        }
    }


}
