package com.amazonaws.s3accessgrants.cache;

import com.amazonaws.services.s3control.model.AWSS3ControlException;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.assertj.core.util.VisibleForTesting;

import static com.amazonaws.s3accessgrants.cache.internal.S3AccessGrantsCacheConstants.ACCESS_DENIED_CACHE_SIZE;
import static com.amazonaws.s3accessgrants.cache.internal.S3AccessGrantsCacheConstants.MAX_LIMIT_ACCESS_GRANTS_MAX_CACHE_SIZE;

import java.util.concurrent.TimeUnit;

public class S3AccessGrantsAccessDeniedCache {
    private Cache<CacheKey, AWSS3ControlException> cache;
    private int maxCacheSize;

    private S3AccessGrantsAccessDeniedCache () {
        this.maxCacheSize = ACCESS_DENIED_CACHE_SIZE;
    }

    public static S3AccessGrantsAccessDeniedCache.Builder builder() {
        return new S3AccessGrantsAccessDeniedCache.BuilderImpl();
    }

    public interface Builder {
        S3AccessGrantsAccessDeniedCache build();
        S3AccessGrantsAccessDeniedCache.Builder maxCacheSize(int maxCacheSize);
    }

    static final class BuilderImpl implements S3AccessGrantsAccessDeniedCache.Builder {

        private int maxCacheSize = ACCESS_DENIED_CACHE_SIZE;
        private BuilderImpl() {
        }

        @Override
        public S3AccessGrantsAccessDeniedCache build() {
            S3AccessGrantsAccessDeniedCache s3AccessGrantsAccessDeniedCache = new S3AccessGrantsAccessDeniedCache();
            s3AccessGrantsAccessDeniedCache.maxCacheSize = maxCacheSize();
            s3AccessGrantsAccessDeniedCache.cache = Caffeine.newBuilder()
                    .maximumSize(maxCacheSize)
                    .expireAfterWrite(5, TimeUnit.MINUTES)
                    .recordStats()
                    .build();

            return s3AccessGrantsAccessDeniedCache;
        }

        @Override
        public Builder maxCacheSize(int maxCacheSize) {
            if (maxCacheSize <= 0 || maxCacheSize > MAX_LIMIT_ACCESS_GRANTS_MAX_CACHE_SIZE) {
                throw new IllegalArgumentException(String.format("maxCacheSize needs to be in range (0, %d]",
                        MAX_LIMIT_ACCESS_GRANTS_MAX_CACHE_SIZE));
            }
            this.maxCacheSize = maxCacheSize;
            return this;
        }

        public int maxCacheSize() {
            return maxCacheSize;
        }
    }

    /**
     * This method throws an exception when there is a cache hit.
     * @param cacheKey CacheKey consists of AwsCredentialsIdentity, Permission, and S3Prefix.
     * @return null
     * @throws AWSS3ControlException when it's a cache hit.
     */
    protected AWSS3ControlException getValueFromCache (CacheKey cacheKey) {
        return cache.getIfPresent(cacheKey);
    }

    /**
     * This method puts an entry in cache.
     * @param cacheKey CacheKey consists of AwsCredentialsIdentity, Permission, and S3Prefix.
     * @param exception The cache value is an Access Denied Exception.
     */
    protected void putValueInCache(CacheKey cacheKey, AWSS3ControlException exception) {
        cache.put(cacheKey, exception);
    }

    /**
     * Invalidates the cache.
     */
    @VisibleForTesting
    void invalidateCache() {
        cache.invalidateAll();
    }

    /***
     * @return metrics captured by the cache
     */
    protected CacheStats getCacheStats() { return cache.stats(); }

}
