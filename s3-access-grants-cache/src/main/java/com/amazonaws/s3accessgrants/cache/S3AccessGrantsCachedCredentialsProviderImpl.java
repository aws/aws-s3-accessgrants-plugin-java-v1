package com.amazonaws.s3accessgrants.cache;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3control.AWSS3Control;
import com.amazonaws.services.s3control.model.AWSS3ControlException;
import com.amazonaws.services.s3control.model.Permission;
import com.amazonaws.thirdparty.apache.logging.Log;
import com.amazonaws.thirdparty.apache.logging.LogFactory;
import com.sun.istack.internal.NotNull;
import org.assertj.core.util.VisibleForTesting;

import static com.amazonaws.s3accessgrants.cache.internal.S3AccessGrantsCacheConstants.CACHE_EXPIRATION_TIME_PERCENTAGE;
import static com.amazonaws.s3accessgrants.cache.internal.S3AccessGrantsCacheConstants.DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE;
import static com.amazonaws.s3accessgrants.cache.internal.S3AccessGrantsCacheConstants.MAX_LIMIT_ACCESS_GRANTS_MAX_CACHE_SIZE;

public class S3AccessGrantsCachedCredentialsProviderImpl implements S3AccessGrantsCachedCredentialsProvider{

    private final S3AccessGrantsCache accessGrantsCache;
    private final S3AccessGrantsAccessDeniedCache s3AccessGrantsAccessDeniedCache;
    private static final Log logger = LogFactory.getLog(S3AccessGrantsCachedCredentialsProviderImpl.class);

    private S3AccessGrantsCachedCredentialsProviderImpl(AWSS3Control s3ControlClient, int maxCacheSize, int cacheExpirationTimePercentage) {

        accessGrantsCache = S3AccessGrantsCache.builder()
                .s3ControlClient(s3ControlClient)
                .maxCacheSize(maxCacheSize)
                .cacheExpirationTimePercentage(cacheExpirationTimePercentage).build();

        s3AccessGrantsAccessDeniedCache = S3AccessGrantsAccessDeniedCache.builder()
                .maxCacheSize(DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE).build();
    }

    @VisibleForTesting
    S3AccessGrantsCachedCredentialsProviderImpl(AWSS3Control s3ControlClient,
                                                S3AccessGrantsCachedAccountIdResolver resolver,int maxCacheSize, int cacheExpirationTimePercentage) {

        accessGrantsCache = S3AccessGrantsCache.builder()
                .s3ControlClient(s3ControlClient)
                .maxCacheSize(maxCacheSize)
                .cacheExpirationTimePercentage(cacheExpirationTimePercentage)
                .s3AccessGrantsCachedAccountIdResolver(resolver)
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
    }

    static final class BuilderImpl implements S3AccessGrantsCachedCredentialsProviderImpl.Builder {
        private AWSS3Control s3ControlClient;
        private S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver;
        private int maxCacheSize = DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE;
        private int cacheExpirationTimePercentage = CACHE_EXPIRATION_TIME_PERCENTAGE;

        private BuilderImpl() {
        }

        @Override
        public S3AccessGrantsCachedCredentialsProviderImpl build() {
            return new S3AccessGrantsCachedCredentialsProviderImpl(s3ControlClient, maxCacheSize, cacheExpirationTimePercentage);
        }

        @Override
        public S3AccessGrantsCachedCredentialsProviderImpl buildWithAccountIdResolver() {
            return new S3AccessGrantsCachedCredentialsProviderImpl(s3ControlClient, s3AccessGrantsCachedAccountIdResolver, maxCacheSize, cacheExpirationTimePercentage);
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
