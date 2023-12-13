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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3control.AWSS3Control;
import com.amazonaws.services.s3control.model.AWSS3ControlException;
import com.amazonaws.services.s3control.model.Permission;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.assertj.core.util.VisibleForTesting;
import javax.validation.constraints.NotNull;

import static com.amazonaws.s3accessgrants.cache.internal.S3AccessGrantsCacheConstants.CACHE_EXPIRATION_TIME_PERCENTAGE;
import static com.amazonaws.s3accessgrants.cache.internal.S3AccessGrantsCacheConstants.DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE;
import static com.amazonaws.s3accessgrants.cache.internal.S3AccessGrantsCacheConstants.MAX_LIMIT_ACCESS_GRANTS_MAX_CACHE_SIZE;
import static com.amazonaws.s3accessgrants.cache.internal.S3AccessGrantsCacheConstants.DEFAULT_DURATION;

public class S3AccessGrantsCachedCredentialsProviderImpl implements S3AccessGrantsCachedCredentialsProvider{

    private final S3AccessGrantsCache accessGrantsCache;
    private final S3AccessGrantsAccessDeniedCache s3AccessGrantsAccessDeniedCache;
    private static final Log logger = LogFactory.getLog(S3AccessGrantsCachedCredentialsProviderImpl.class);

    private S3AccessGrantsCachedCredentialsProviderImpl(AWSS3Control s3ControlClient, int maxCacheSize, int cacheExpirationTimePercentage, int duration) {

        accessGrantsCache = S3AccessGrantsCache.builder()
                .s3ControlClient(s3ControlClient)
                .maxCacheSize(maxCacheSize)
                .duration(duration)
                .cacheExpirationTimePercentage(cacheExpirationTimePercentage).build();

        s3AccessGrantsAccessDeniedCache = S3AccessGrantsAccessDeniedCache.builder()
                .maxCacheSize(DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE).build();
    }

    @VisibleForTesting
    S3AccessGrantsCachedCredentialsProviderImpl(AWSS3Control s3ControlClient,
                                                S3AccessGrantsCachedAccountIdResolver resolver,int maxCacheSize, int cacheExpirationTimePercentage, int duration) {

        accessGrantsCache = S3AccessGrantsCache.builder()
                .s3ControlClient(s3ControlClient)
                .maxCacheSize(maxCacheSize)
                .cacheExpirationTimePercentage(cacheExpirationTimePercentage)
                .s3AccessGrantsCachedAccountIdResolver(resolver)
                .duration(duration)
                .buildWithAccountIdResolver();
        s3AccessGrantsAccessDeniedCache = S3AccessGrantsAccessDeniedCache.builder()
                .maxCacheSize(DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE).build();
    }

    public static S3AccessGrantsCachedCredentialsProviderImpl.Builder builder() {
        return new S3AccessGrantsCachedCredentialsProviderImpl.BuilderImpl();
    }

    public interface Builder {
        S3AccessGrantsCachedCredentialsProviderImpl build();
        S3AccessGrantsCachedCredentialsProviderImpl buildWithAccountIdResolver();
        S3AccessGrantsCachedCredentialsProviderImpl.Builder s3ControlClient(AWSS3Control s3ControlClient);
        S3AccessGrantsCachedCredentialsProviderImpl.Builder s3AccessGrantsCachedAccountIdResolver(S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver);
        S3AccessGrantsCachedCredentialsProviderImpl.Builder maxCacheSize(int maxCacheSize);
        S3AccessGrantsCachedCredentialsProviderImpl.Builder cacheExpirationTimePercentage(int cacheExpirationTimePercentage);
        S3AccessGrantsCachedCredentialsProviderImpl.Builder duration(int duration);
    }

    static final class BuilderImpl implements S3AccessGrantsCachedCredentialsProviderImpl.Builder {
        private AWSS3Control s3ControlClient;
        private S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver;
        private int maxCacheSize = DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE;
        private int cacheExpirationTimePercentage = CACHE_EXPIRATION_TIME_PERCENTAGE;
        private int duration = DEFAULT_DURATION;

        private BuilderImpl() {
        }

        @Override
        public S3AccessGrantsCachedCredentialsProviderImpl build() {
            return new S3AccessGrantsCachedCredentialsProviderImpl(s3ControlClient, maxCacheSize, cacheExpirationTimePercentage, duration);
        }

        @Override
        public S3AccessGrantsCachedCredentialsProviderImpl buildWithAccountIdResolver() {
            return new S3AccessGrantsCachedCredentialsProviderImpl(s3ControlClient, s3AccessGrantsCachedAccountIdResolver, maxCacheSize, cacheExpirationTimePercentage, duration);
        }

        @Override
        public S3AccessGrantsCachedCredentialsProviderImpl.Builder s3ControlClient(AWSS3Control s3ControlClient) {
            this.s3ControlClient = s3ControlClient;
            return this;
        }

        @Override
        public S3AccessGrantsCachedCredentialsProviderImpl.Builder s3AccessGrantsCachedAccountIdResolver(S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver) {
            this.s3AccessGrantsCachedAccountIdResolver = s3AccessGrantsCachedAccountIdResolver;
            return this;
        }

        @Override
        public Builder maxCacheSize(int maxCacheSize) {
            if (maxCacheSize <= 0 || maxCacheSize > MAX_LIMIT_ACCESS_GRANTS_MAX_CACHE_SIZE) {
                throw new IllegalArgumentException(String.format("maxCacheSize needs to be in range (0, %d]",
                        MAX_LIMIT_ACCESS_GRANTS_MAX_CACHE_SIZE));
            }
            return this;
        }

        @Override
        public Builder cacheExpirationTimePercentage(int cacheExpirationTimePercentage) {
            if (cacheExpirationTimePercentage <= 0 || (float) cacheExpirationTimePercentage > DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE) {
                throw new IllegalArgumentException(String.format("maxExpirationTimePercentage needs to be in range (0, %d]",
                        DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE));
            }
            this.cacheExpirationTimePercentage = cacheExpirationTimePercentage;
            return this;
        }

        @Override
        public Builder duration(int duration) {
            this.duration = duration;
            return this;
        }

    }

    @Override
    public AWSCredentials getDataAccess (AWSCredentials credentials, Permission permission,
                                                String s3Prefix, @NotNull String accountId) throws AWSS3ControlException {

        CacheKey cacheKey = CacheKey.builder()
                .credentials(credentials)
                .permission(permission)
                .s3Prefix(s3Prefix).build();

        AWSS3ControlException s3ControlException = s3AccessGrantsAccessDeniedCache.getValueFromCache(cacheKey);
        if (s3ControlException != null) {
            logger.debug("Found a matching request in the cache which was denied.");
            logger.error("Exception occurred while fetching the credentials: " + s3ControlException);
            throw s3ControlException;
        }
        AWSCredentials accessGrantsCredentials;
        try {
            accessGrantsCredentials = accessGrantsCache.getCredentials(cacheKey, accountId, s3AccessGrantsAccessDeniedCache);
        }catch (AWSS3ControlException e) {
            throw e;
        }
        return accessGrantsCredentials;
    }

    public void invalidateCache() {
        accessGrantsCache.invalidateCache();
    }

}
