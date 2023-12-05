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

import com.amazonaws.arn.Arn;
import com.amazonaws.services.s3control.AWSS3Control;
import com.amazonaws.services.s3control.model.AWSS3ControlException;
import com.amazonaws.services.s3control.model.GetAccessGrantsInstanceForPrefixRequest;
import com.amazonaws.services.s3control.model.GetAccessGrantsInstanceForPrefixResult;
import com.amazonaws.thirdparty.apache.logging.Log;
import com.amazonaws.thirdparty.apache.logging.LogFactory;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.sun.istack.internal.NotNull;
import org.assertj.core.util.VisibleForTesting;
import java.time.Duration;

import static com.amazonaws.s3accessgrants.cache.internal.S3AccessGrantsCacheConstants.DEFAULT_ACCOUNT_ID_MAX_CACHE_SIZE;
import static com.amazonaws.s3accessgrants.cache.internal.S3AccessGrantsCacheConstants.DEFAULT_ACCOUNT_ID_EXPIRE_CACHE_AFTER_WRITE_SECONDS;
import static com.amazonaws.s3accessgrants.cache.internal.S3AccessGrantsCacheConstants.MAX_LIMIT_ACCOUNT_ID_MAX_CACHE_SIZE;
import static com.amazonaws.s3accessgrants.cache.internal.S3AccessGrantsCacheConstants.MAX_LIMIT_ACCOUNT_ID_EXPIRE_CACHE_AFTER_WRITE_SECONDS;
import static com.amazonaws.s3accessgrants.cache.internal.S3AccessGrantsCacheUtils.getBucketName;

public class S3AccessGrantsCachedAccountIdResolver implements S3AccessGrantsAccountIdResolver {

    private final AWSS3Control s3ControlClient;
    private int maxCacheSize;
    private int expireCacheAfterWriteSeconds;
    private static final Log logger = LogFactory.getLog(S3AccessGrantsCachedAccountIdResolver.class);
    private Cache<String, String> cache;

    public AWSS3Control s3ControlClient() {
        return s3ControlClient;
    }

    public int maxCacheSize() {
        return maxCacheSize;
    }

    public int expireCacheAfterWriteSeconds() {
        return expireCacheAfterWriteSeconds;
    }

    protected CacheStats getCacheStats() { return cache.stats(); }

    @VisibleForTesting
    S3AccessGrantsCachedAccountIdResolver(@NotNull AWSS3Control s3ControlClient) {
        if (s3ControlClient == null) {
            throw new IllegalArgumentException("S3ControlClient is required");
        }
        this.s3ControlClient = s3ControlClient;
        this.maxCacheSize = DEFAULT_ACCOUNT_ID_MAX_CACHE_SIZE;
        this.expireCacheAfterWriteSeconds = DEFAULT_ACCOUNT_ID_EXPIRE_CACHE_AFTER_WRITE_SECONDS;
    }


    public Builder toBuilder() {
        return new BuilderImpl(this);
    }

    public static Builder builder() {
        return new BuilderImpl();
    }

    @Override
    public String resolve(String accountId, String s3Prefix) {
        String bucketName = getBucketName(s3Prefix);
        String s3PrefixAccountId = cache.getIfPresent(bucketName);
        if (s3PrefixAccountId == null) {
            logger.debug("Account Id not available in the cache. Fetching account from server.");
            s3PrefixAccountId = resolveFromService(accountId, s3Prefix);
            cache.put(bucketName, s3PrefixAccountId);
        }
        return s3PrefixAccountId;
    }

    /**
     * @param accountId AWS AccountId from the request context parameter
     * @param s3Prefix e.g., s3://bucket-name/path/to/helloworld.txt
     * @return accountId from the service response
     */
    private String resolveFromService(String accountId, String s3Prefix) {
        GetAccessGrantsInstanceForPrefixResult accessGrantsInstanceForPrefix =
                s3ControlClient.getAccessGrantsInstanceForPrefix(new GetAccessGrantsInstanceForPrefixRequest()
                        .withS3Prefix(s3Prefix).withAccountId(accountId));
        String accessGrantsInstanceArn = accessGrantsInstanceForPrefix.getAccessGrantsInstanceArn();
        try {
            Arn arn = Arn.fromString(accessGrantsInstanceArn);
            return arn.getAccountId();
        } catch (IllegalArgumentException e) {
            throw new AWSS3ControlException("accessGrantsInstanceArn is empty");
        }
    }

    public interface Builder {
        S3AccessGrantsCachedAccountIdResolver build();

        Builder s3ControlClient(AWSS3Control s3ControlClient);

        Builder maxCacheSize(int maxCacheSize);

        Builder expireCacheAfterWriteSeconds(int expireCacheAfterWriteSeconds);
    }

    static final class BuilderImpl implements Builder {
        private AWSS3Control s3ControlClient;
        private int maxCacheSize = DEFAULT_ACCOUNT_ID_MAX_CACHE_SIZE;
        private int expireCacheAfterWriteSeconds = DEFAULT_ACCOUNT_ID_EXPIRE_CACHE_AFTER_WRITE_SECONDS;

        private BuilderImpl() {
        }

        public BuilderImpl(S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver) {
            s3ControlClient(s3AccessGrantsCachedAccountIdResolver.s3ControlClient);
            maxCacheSize(s3AccessGrantsCachedAccountIdResolver.maxCacheSize);
            expireCacheAfterWriteSeconds(s3AccessGrantsCachedAccountIdResolver.expireCacheAfterWriteSeconds);
        }

        @Override
        public Builder s3ControlClient(AWSS3Control s3ControlClient) {
            this.s3ControlClient = s3ControlClient;
            return this;
        }

        public int maxCacheSize() {
            return maxCacheSize;
        }

        @Override
        public Builder maxCacheSize(int maxCacheSize) {
            if (maxCacheSize <= 0 || maxCacheSize > MAX_LIMIT_ACCOUNT_ID_MAX_CACHE_SIZE) {
                throw new IllegalArgumentException(String.format("maxCacheSize needs to be in range (0, %d]",
                        MAX_LIMIT_ACCOUNT_ID_MAX_CACHE_SIZE));
            }
            this.maxCacheSize = maxCacheSize;
            return this;
        }

        public int expireCAcheAfterWriteSeconds() {
            return expireCacheAfterWriteSeconds;
        }

        @Override
        public Builder expireCacheAfterWriteSeconds(int expireCacheAfterWriteSeconds) {
            if (expireCacheAfterWriteSeconds <= 0 || expireCacheAfterWriteSeconds > MAX_LIMIT_ACCOUNT_ID_EXPIRE_CACHE_AFTER_WRITE_SECONDS) {
                throw new IllegalArgumentException(String.format("expireCacheAfterWriteSeconds needs to be in range (0, %d]",
                        MAX_LIMIT_ACCOUNT_ID_EXPIRE_CACHE_AFTER_WRITE_SECONDS));
            }
            this.expireCacheAfterWriteSeconds = expireCacheAfterWriteSeconds;
            return this;
        }

        @Override
        public S3AccessGrantsCachedAccountIdResolver build() {
            S3AccessGrantsCachedAccountIdResolver resolver = new S3AccessGrantsCachedAccountIdResolver(s3ControlClient);
            resolver.maxCacheSize = maxCacheSize();
            resolver.expireCacheAfterWriteSeconds = expireCAcheAfterWriteSeconds();
            resolver.cache = Caffeine.newBuilder()
                    .maximumSize(maxCacheSize)
                    .expireAfterWrite(Duration.ofSeconds(expireCacheAfterWriteSeconds))
                    .build();
            return resolver;
        }
    }
}
