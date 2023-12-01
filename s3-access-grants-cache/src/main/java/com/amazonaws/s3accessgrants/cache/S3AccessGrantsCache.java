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
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.s3control.AWSS3Control;
import com.amazonaws.services.s3control.model.AWSS3ControlException;
import com.amazonaws.services.s3control.model.Credentials;
import com.amazonaws.services.s3control.model.GetDataAccessRequest;
import com.amazonaws.services.s3control.model.GetDataAccessResult;
import com.amazonaws.services.s3control.model.Permission;
import com.amazonaws.services.s3control.model.Privilege;
import com.amazonaws.thirdparty.apache.logging.Log;
import com.amazonaws.thirdparty.apache.logging.LogFactory;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.sun.istack.internal.NotNull;
import org.assertj.core.util.VisibleForTesting;

import static com.amazonaws.s3accessgrants.cache.internal.S3AccessGrantsCacheConstants.DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

public class S3AccessGrantsCache {
    private Cache<CacheKey, AWSCredentials> cache;
    private final AWSS3Control s3ControlClient;
    private int maxCacheSize;
    private final S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver;
    private final int cacheExpirationTimePercentage;
    private static final Log logger = LogFactory.getLog(S3AccessGrantsCache.class);

    private S3AccessGrantsCache (@NotNull AWSS3Control s3ControlClient,
                                 S3AccessGrantsCachedAccountIdResolver resolver, int maxCacheSize, int cacheExpirationTimePercentage) {
        if (s3ControlClient == null) {
            throw new IllegalArgumentException("S3ControlClient is required");
        }
        this.s3ControlClient = s3ControlClient;
        this.s3AccessGrantsCachedAccountIdResolver = resolver;
        this.cacheExpirationTimePercentage = cacheExpirationTimePercentage;
        this.maxCacheSize = maxCacheSize;
        this.cache = Caffeine.newBuilder().maximumSize(maxCacheSize)
                .expireAfter(new CustomCacheExpiry<>())
                .recordStats()
                .build();
    }

    protected static S3AccessGrantsCache.Builder builder() {
        return new S3AccessGrantsCache.BuilderImpl();
    }

    public interface Builder {
        S3AccessGrantsCache build();
        S3AccessGrantsCache buildWithAccountIdResolver();
        S3AccessGrantsCache.Builder s3ControlClient(AWSS3Control s3ControlClient);
        S3AccessGrantsCache.Builder maxCacheSize(int maxCacheSize);
        S3AccessGrantsCache.Builder cacheExpirationTimePercentage(int cacheExpirationTimePercentage);
        S3AccessGrantsCache.Builder s3AccessGrantsCachedAccountIdResolver(S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver);
    }

    static final class BuilderImpl implements S3AccessGrantsCache.Builder {
        private AWSS3Control s3ControlClient;
        private int maxCacheSize = DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE;
        private S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver;
        private int cacheExpirationTimePercentage;

        private BuilderImpl() {
        }

        @Override
        public S3AccessGrantsCache build() {
            S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver =
                    S3AccessGrantsCachedAccountIdResolver.builder().s3ControlClient(s3ControlClient).build();
            return new S3AccessGrantsCache(s3ControlClient, s3AccessGrantsCachedAccountIdResolver, maxCacheSize, cacheExpirationTimePercentage);
        }

        @Override
        public S3AccessGrantsCache buildWithAccountIdResolver() {
            return new S3AccessGrantsCache(s3ControlClient, s3AccessGrantsCachedAccountIdResolver, maxCacheSize,
                    cacheExpirationTimePercentage);
        }

        @Override
        public Builder s3ControlClient(AWSS3Control s3ControlClient) {
            this.s3ControlClient = s3ControlClient;
            return this;
        }

        @Override
        public Builder maxCacheSize(int maxCacheSize) {
            this.maxCacheSize = maxCacheSize;
            return this;
        }

        @Override
        public Builder cacheExpirationTimePercentage(int cacheExpirationTimePrecentage) {
            this.cacheExpirationTimePercentage = cacheExpirationTimePrecentage;
            return this;
        }

        @Override
        public Builder s3AccessGrantsCachedAccountIdResolver(S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver) {
            this.s3AccessGrantsCachedAccountIdResolver = s3AccessGrantsCachedAccountIdResolver;
            return this;
        }
    }

    /**
     * This method searches for the cacheKey in the cache. It will also search for a cache key with broader permission than
     * requested.
     * @param cacheKey CacheKey consists of AwsCredentialsIdentity, Permission, and S3Prefix.
     * @param accountId Account Id of the requester
     * @param s3AccessGrantsAccessDeniedCache instance of S3AccessGrantsAccessDeniedCache
     * @return cached Access Grants credentials.
     */
    protected AWSCredentials getCredentials (CacheKey cacheKey, String accountId,
                                                                        S3AccessGrantsAccessDeniedCache s3AccessGrantsAccessDeniedCache) throws AWSS3ControlException {

        logger.debug("Fetching credentials from Access Grants for s3Prefix: " + cacheKey.s3Prefix);

        AWSCredentials credentials = searchKeyInCacheAtPrefixLevel(cacheKey);
        if (credentials == null &&
                (cacheKey.permission == Permission.READ ||
                        cacheKey.permission == Permission.WRITE)) {
            credentials = searchKeyInCacheAtPrefixLevel(cacheKey.toBuilder().permission(Permission.READWRITE).build());
        }
        if (credentials == null) {
            credentials = searchKeyInCacheAtCharacterLevel(cacheKey);
        }
        if (credentials == null &&
                (cacheKey.permission == Permission.READ ||
                        cacheKey.permission == Permission.WRITE)) {
            credentials = searchKeyInCacheAtCharacterLevel(cacheKey.toBuilder().permission(Permission.READWRITE).build());
        }
        if (credentials == null) {
            try {
                logger.debug("Credentials not available in the cache. Fetching credentials from Access Grants service.");
                GetDataAccessResult getDataAccessResult = getCredentialsFromService(cacheKey, accountId);
                Credentials accessGrantsCredentials = getDataAccessResult.getCredentials();
                long duration = getTTL(accessGrantsCredentials.getExpiration().toInstant());
                AWSCredentials sessionCredentials = new BasicSessionCredentials(accessGrantsCredentials.getAccessKeyId(),
                        accessGrantsCredentials.getSecretAccessKey(), accessGrantsCredentials.getSessionToken());
                String accessGrantsTarget = getDataAccessResult.getMatchedGrantTarget();
                if (accessGrantsTarget.endsWith("*")) {
                    putValueInCache(cacheKey.toBuilder().s3Prefix(processMatchedGrantTarget(accessGrantsTarget)).build(), sessionCredentials, duration);
                }
                logger.debug("Successfully retrieved the credentials from Access Grants service");
                return sessionCredentials;
            } catch (AWSS3ControlException s3ControlException) {
                logger.error("Exception occurred while fetching the credentials: " + s3ControlException);
                if (s3ControlException.getStatusCode()== 403) {
                    logger.debug("Caching the Access Denied request.");
                    s3AccessGrantsAccessDeniedCache.putValueInCache(cacheKey, s3ControlException);
                }
                throw s3ControlException;
            }
        }
        return credentials;
    }

    /**
     * This method calculates the TTL of a cache entry
     * @param expirationTime of the credentials received from Access Grants
     * @return TTL of a cache entry
     */
    @VisibleForTesting
    long getTTL(Instant expirationTime) {
        Instant now = Instant.now();
        return (long) ((expirationTime.getEpochSecond() - now.getEpochSecond()) * (cacheExpirationTimePercentage / 100.0f));
    }

    /**
     * This method calls Access Grants service to get the credentials.
     * @param cacheKey CacheKey consists of AwsCredentialsIdentity, Permission, and S3Prefix.
     * @param accountId Account Id of the requester.
     * @return Access Grants Credentials.
     * @throws AWSS3ControlException throws Exception received from service.
     */
    private GetDataAccessResult getCredentialsFromService(CacheKey cacheKey, String accountId) throws AWSS3ControlException{
        String resolvedAccountId = s3AccessGrantsCachedAccountIdResolver.resolve(accountId, cacheKey.s3Prefix);
        logger.debug("Fetching credentials from Access Grants for accountId: " + resolvedAccountId + ", s3Prefix: " + cacheKey.s3Prefix +
                ", permission: " + cacheKey.permission + ", privilege: " + Privilege.Default);
        GetDataAccessRequest dataAccessRequest = new GetDataAccessRequest()
                .withAccountId(resolvedAccountId)
                .withTarget(cacheKey.s3Prefix)
                .withPermission(cacheKey.permission)
                .withPrivilege(Privilege.Default);

        return s3ControlClient.getDataAccess(dataAccessRequest);
    }

    /**
     * This method searches for the cacheKey in the cache. It will also search for a cache key with higher S3 prefix than
     * requested.
     * @param cacheKey CacheKey consists of AwsCredentialsIdentity, Permission, and S3Prefix.
     * @return cached Access Grants credentials.
     */
    private AWSCredentials searchKeyInCacheAtPrefixLevel (CacheKey cacheKey) {
        AWSCredentials cacheValue;
        String prefix = cacheKey.s3Prefix;
        while (!prefix.equals("s3:")) {
            CacheKey key = cacheKey.toBuilder().s3Prefix(prefix).build();
            cacheValue = cache.getIfPresent(key);
            if (cacheValue != null) {
                logger.debug("Successfully retrieved credentials from the cache.");
                return cacheValue;
            }
            prefix = getNextPrefix(prefix);
        }
        return null;
    }

    /**
     * This method looks for grants present in the cache of type "s3://bucketname/foo*"
     * @param cacheKey CacheKey consists of AwsCredentialsIdentity, Permission, and S3Prefix.
     * @return cached Access Grants credentials.
     */
    private AWSCredentials searchKeyInCacheAtCharacterLevel (CacheKey cacheKey) {
        AWSCredentials cacheValue;
        String prefix = cacheKey.s3Prefix;
        while (!prefix.equals("s3://")) {
            cacheValue = cache.getIfPresent(cacheKey.toBuilder().s3Prefix(prefix + "*").build());
            if (cacheValue != null) {
                logger.debug("Successfully retrieved credentials from the cache.");
                return cacheValue;
            }
            prefix = getNextPrefixByChar(prefix);
        }
        return null;
    }

    /**
     * This method puts an entry in cache.
     * @param cacheKey CacheKey consists of AwsCredentialsIdentity, Permission, and S3Prefix.
     * @param credentials The cache value credentials returned by Access Grants.
     * @param duration TTL for the cache entry.
     */
    @VisibleForTesting
    void putValueInCache(CacheKey cacheKey, AWSCredentials credentials, long duration) {
        logger.debug("Caching the credentials for s3Prefix:" + cacheKey.s3Prefix
                + " and permission: " + cacheKey.permission);
        cache.put(cacheKey, credentials);
        cache.policy().expireVariably().ifPresent(ev -> ev.setExpiresAfter(cacheKey, duration, TimeUnit.SECONDS));
    }

    /**
     * This method splits S3Prefix on last "/" and returns the first part.
     */
    private String getNextPrefix(String prefix) {
        return prefix.substring(0, prefix.lastIndexOf("/"));
    }

    /**
     * This methods returns a substring of the string with last character removed.
     */
    private String getNextPrefixByChar(String prefix) {
        return prefix.substring(0, prefix.length()-1);
    }

    /**
     * This method removes '/*' from matchedGrantTarget if present
     * @param matchedGrantTarget from Access Grants response
     * @return a clean version of matchedGrantTarget
     */
    @VisibleForTesting
    String processMatchedGrantTarget(String matchedGrantTarget) {
        if (matchedGrantTarget.substring(matchedGrantTarget.length() - 2).equals("/*")) {
            return matchedGrantTarget.substring(0, matchedGrantTarget.length() - 2);
        }
        return matchedGrantTarget;
    }

    /**
     * Invalidates the cache.
     */
    @VisibleForTesting
    void invalidateCache() {
        cache.invalidateAll();
    }

    public Cache getCache() {
        return cache;
    }

    private static class CustomCacheExpiry<K, V> implements Expiry<K, V> {

        @Override
        public long expireAfterCreate(K key, V value, long currentTime) {
            return Long.MIN_VALUE;  // Keep min by default
        }

        @Override
        public long expireAfterUpdate(K key, V value, long currentTime, long currentDuration) {
            return currentDuration;  // Retain original expiration time if updated
        }

        @Override
        public long expireAfterRead(K key, V value, long currentTime, long currentDuration) {
            return currentDuration;  // Retain original expiration time if read
        }
    }
}

