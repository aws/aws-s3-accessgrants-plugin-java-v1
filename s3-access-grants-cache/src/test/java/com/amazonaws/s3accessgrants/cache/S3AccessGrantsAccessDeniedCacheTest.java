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

import com.amazonaws.services.s3control.model.AWSS3ControlException;
import com.amazonaws.services.s3control.model.Permission;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static com.amazonaws.s3accessgrants.cache.internal.S3AccessGrantsCacheConstants.DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE;
import static com.amazonaws.s3accessgrants.cache.S3AccessGrantsTestConstants.AWS_SESSION_CREDENTIALS;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertThrows;

public class S3AccessGrantsAccessDeniedCacheTest {
    static S3AccessGrantsAccessDeniedCache cache;
    AWSS3ControlException s3ControlException = Mockito.mock(AWSS3ControlException.class);
    @Before
    public void setup() {
        cache = S3AccessGrantsAccessDeniedCache.builder().maxCacheSize(DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE).build();
    }

    @Before
    public void clearCache() {
        cache.invalidateCache();
    }

    @Test
    public void accessDeniedCache_accessGrantsCacheHit() {
        // Given
        CacheKey key1 = CacheKey.builder()
                .credentials(AWS_SESSION_CREDENTIALS)
                .permission(Permission.READ)
                .s3Prefix("s3://bucket2/foo/bar").build();
        cache.putValueInCache(key1,s3ControlException);
        // When
        CacheKey key2 = CacheKey.builder()
                .credentials(AWS_SESSION_CREDENTIALS)
                .permission(Permission.READ)
                .s3Prefix("s3://bucket2/foo/bar").build();
        // Then
        assertThat(cache.getValueFromCache(key2)).isInstanceOf(AWSS3ControlException.class);

    }

    @Test
    public void accessDeniedCache_accessGrantsCacheMiss() {
        // Given
        CacheKey key1 = CacheKey.builder()
                .credentials(AWS_SESSION_CREDENTIALS)
                .permission(Permission.READ)
                .s3Prefix("s3://bucket1/foo/bar").build();
        // When the key is not present in the cache then
        assertThat(cache.getValueFromCache(key1)).isNull();
    }

    @Test
    public void accessDeniedCache_grantNotPresentOnLowerLevelPrefix() {
        // Given
        CacheKey key1 = CacheKey.builder()
                .credentials(AWS_SESSION_CREDENTIALS)
                .permission(Permission.READ)
                .s3Prefix("s3://bucket2/foo/bar").build();
        // When
        cache.putValueInCache(key1,s3ControlException);
        CacheKey key2 = CacheKey.builder()
                .credentials(AWS_SESSION_CREDENTIALS)
                .permission(Permission.READ)
                .s3Prefix("s3://bucket2/foo/bar/log").build();
        // Then
        assertThat(cache.getValueFromCache(key2)).isNull();

    }

    @Test
    public void accessDeniedCache_throwsErrorForCacheSizeBiggerThanMaxCacheSize() {
        assertThrows(IllegalArgumentException.class, () -> S3AccessGrantsAccessDeniedCache.builder()
                .maxCacheSize(1_000_020).build());
    }
}
