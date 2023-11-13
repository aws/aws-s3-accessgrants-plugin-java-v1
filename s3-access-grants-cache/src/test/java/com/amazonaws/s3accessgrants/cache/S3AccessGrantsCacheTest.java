package com.amazonaws.s3accessgrants.cache;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.s3control.AWSS3Control;
import com.amazonaws.services.s3control.model.AWSS3ControlException;
import com.amazonaws.services.s3control.model.Credentials;
import com.amazonaws.services.s3control.model.GetDataAccessRequest;
import com.amazonaws.services.s3control.model.GetDataAccessResult;
import com.amazonaws.services.s3control.model.Permission;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Date;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static com.amazonaws.s3accessgrants.cache.internal.S3AccessGrantsCacheConstants.DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE;
import static com.amazonaws.s3accessgrants.cache.S3AccessGrantsTestConstants.AWS_SESSION_CREDENTIALS;
import static com.amazonaws.s3accessgrants.cache.S3AccessGrantsTestConstants.ACCESS_KEY_ID;
import static com.amazonaws.s3accessgrants.cache.S3AccessGrantsTestConstants.SESSION_TOKEN;
import static com.amazonaws.s3accessgrants.cache.S3AccessGrantsTestConstants.SECRET_ACCESS_KEY;
import static com.amazonaws.s3accessgrants.cache.S3AccessGrantsTestConstants.TEST_S3_ACCESSGRANTS_ACCOUNT;
import static com.amazonaws.s3accessgrants.cache.S3AccessGrantsTestConstants.S3_ACCESS_GRANTS_CREDENTIALS;
import static com.amazonaws.s3accessgrants.cache.S3AccessGrantsTestConstants.AWS_BASIC_CREDENTIALS;

public class S3AccessGrantsCacheTest {
    private S3AccessGrantsCache cache;
    private S3AccessGrantsCache cacheWithMockedAccountIdResolver;
    private S3AccessGrantsAccessDeniedCache accessDeniedCache;
    private AWSS3Control s3ControlClient;
    private S3AccessGrantsCachedAccountIdResolver mockResolver;

    @Before
    public void setup(){
        mockResolver = Mockito.mock(S3AccessGrantsCachedAccountIdResolver.class);
        s3ControlClient = Mockito.mock(AWSS3Control.class);
        cache = S3AccessGrantsCache.builder()
                .s3ControlClient(s3ControlClient)
                .cacheExpirationTimePercentage(60)
                .maxCacheSize(DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE).build();
        cacheWithMockedAccountIdResolver = S3AccessGrantsCache.builder()
                .s3ControlClient(s3ControlClient)
                .cacheExpirationTimePercentage(60)
                .s3AccessGrantsCachedAccountIdResolver(mockResolver)
                .maxCacheSize(DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE).buildWithAccountIdResolver();
        accessDeniedCache = S3AccessGrantsAccessDeniedCache.builder().maxCacheSize(DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE).build();

    }

    @After
    public void clearCache(){
        cache.invalidateCache();
        cacheWithMockedAccountIdResolver.invalidateCache();
        accessDeniedCache.invalidateCache();
    }

    private GetDataAccessResult getDataAccessResponseSetUp(String s3Prefix) {
        Instant ttl  = Instant.now().plus(Duration.ofMinutes(1));
        Credentials credentials = new Credentials().withAccessKeyId(ACCESS_KEY_ID)
                .withSecretAccessKey(SECRET_ACCESS_KEY)
                .withSessionToken(SESSION_TOKEN)
                .withExpiration(Date.from(ttl));
        return new GetDataAccessResult()
                .withCredentials(credentials)
                .withMatchedGrantTarget(s3Prefix + "/*");
    }



    @Test
    public void accessGrantsCache_accessGrantsCacheHit() {
        // Given
        CacheKey key1 = CacheKey.builder()
                .credentials(AWS_SESSION_CREDENTIALS)
                .permission(Permission.READ)
                .s3Prefix("s3://bucket2/foo/bar").build();
        cache.putValueInCache(key1, S3_ACCESS_GRANTS_CREDENTIALS, 10);
        AWSSessionCredentials sessionCredentials = new BasicSessionCredentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY, SESSION_TOKEN);
        CacheKey key2 = CacheKey.builder()
                .credentials(sessionCredentials)
                .permission(Permission.READ)
                .s3Prefix("s3://bucket2/foo/bar").build();
        // When
        AWSCredentials cacheValue2 = cache.getCredentials(key2, TEST_S3_ACCESSGRANTS_ACCOUNT, accessDeniedCache);
        AWSCredentials cacheValue1 = cache.getCredentials(key1, TEST_S3_ACCESSGRANTS_ACCOUNT, accessDeniedCache);

        // Then
        assertThat(cacheValue2).isEqualTo(cacheValue1);
    }

    @Test
    public void accessGrantsCache_grantPresentForHigherLevelPrefix() {
        // Given
        CacheKey key1 = CacheKey.builder()
                .credentials(AWS_SESSION_CREDENTIALS)
                .permission(Permission.READ)
                .s3Prefix("s3://bucket2/foo/bar").build();
        cache.putValueInCache(key1, S3_ACCESS_GRANTS_CREDENTIALS, 2);

        AWSSessionCredentials sessionCredentials = new BasicSessionCredentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY, SESSION_TOKEN);
                CacheKey key2 = CacheKey.builder()
                .credentials(sessionCredentials)
                .permission(Permission.READ)
                .s3Prefix("s3://bucket2/foo/bar/logs").build();
        // When
        AWSCredentials cacheValue1 = cache.getCredentials(key1, TEST_S3_ACCESSGRANTS_ACCOUNT, accessDeniedCache);
        AWSCredentials cacheValue2 = cache.getCredentials(key2, TEST_S3_ACCESSGRANTS_ACCOUNT, accessDeniedCache);
        // Then
        assertThat(cacheValue2).isEqualTo(cacheValue1);
    }

    @Test
    public void accessGrantsCache_readRequestShouldCheckForExistingReadWriteGrant() {
        // Given
        CacheKey key1 = CacheKey.builder()
                .credentials(AWS_BASIC_CREDENTIALS)
                .permission(Permission.READWRITE)
                .s3Prefix("s3://bucket2/foo/bar").build();
        cache.putValueInCache(key1, S3_ACCESS_GRANTS_CREDENTIALS, 2);

        BasicAWSCredentials credentials = new BasicAWSCredentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY);
        CacheKey key2 = CacheKey.builder()
                .credentials(credentials)
                .permission(Permission.READ)
                .s3Prefix("s3://bucket2/foo/bar/logs").build();
        // When
        AWSCredentials cacheValue1 = cache.getCredentials(key1, TEST_S3_ACCESSGRANTS_ACCOUNT, accessDeniedCache);
        AWSCredentials cacheValue2 = cache.getCredentials(key2, TEST_S3_ACCESSGRANTS_ACCOUNT, accessDeniedCache);
        // Then
        assertThat(cacheValue2).isEqualTo(cacheValue1);
    }

    @Test
    public void accessGrantsCache_testCacheExpiry() throws Exception {
        // Given
        CacheKey key1 = CacheKey.builder()
                .credentials(AWS_SESSION_CREDENTIALS)
                .permission(Permission.READ)
                .s3Prefix("s3://bucket2/foo/bar").build();
        cache.putValueInCache(key1, S3_ACCESS_GRANTS_CREDENTIALS,2);

        AWSSessionCredentials sessionCredentials = new BasicSessionCredentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY, SESSION_TOKEN);
        CacheKey key2 = CacheKey.builder()
                .credentials(sessionCredentials)
                .permission(Permission.READ)
                .s3Prefix("s3://bucket2/foo/bar").build();

        assertThat(S3_ACCESS_GRANTS_CREDENTIALS).isEqualTo(cache.getCredentials(key2, TEST_S3_ACCESSGRANTS_ACCOUNT,
                accessDeniedCache));
        // When
        Thread.sleep(3000);
        when(mockResolver.resolve(any(String.class), any(String.class))).thenReturn(TEST_S3_ACCESSGRANTS_ACCOUNT);
        when(s3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(getDataAccessResponseSetUp("s3://bucket2/foo/bar"));
        cacheWithMockedAccountIdResolver.getCredentials(key1, TEST_S3_ACCESSGRANTS_ACCOUNT, accessDeniedCache);
        // Then
        verify(s3ControlClient, atLeastOnce()).getDataAccess(any(GetDataAccessRequest.class));
    }


    @Test
    public void accessGrantsCache_accessGrantsCacheMiss() {
        // Given
        CacheKey key = CacheKey.builder()
                .credentials(S3_ACCESS_GRANTS_CREDENTIALS)
                .permission(Permission.READ)
                .s3Prefix("s3://bucket/foo/bar").build();
        GetDataAccessResult getDataAccessResponse = getDataAccessResponseSetUp("s3://bucket2/foo/bar");

        when(mockResolver.resolve(any(String.class), any(String.class))).thenReturn(TEST_S3_ACCESSGRANTS_ACCOUNT);
        when(s3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(getDataAccessResponse);
        // When
        cacheWithMockedAccountIdResolver.getCredentials(key, TEST_S3_ACCESSGRANTS_ACCOUNT, accessDeniedCache);
        // Then
        verify(s3ControlClient, atLeastOnce()).getDataAccess(any(GetDataAccessRequest.class));
    }

    @Test
    public void accessGrantsCache_accessGrantsCacheMissForDifferentPermissions() {
        // Given
        CacheKey key1 = CacheKey.builder()
                .credentials(AWS_BASIC_CREDENTIALS)
                .permission(Permission.READ)
                .s3Prefix("s3://bucket2/foo/bar").build();
        cache.putValueInCache(key1, S3_ACCESS_GRANTS_CREDENTIALS, 2);

        CacheKey key2 = CacheKey.builder()
                .credentials(S3_ACCESS_GRANTS_CREDENTIALS)
                .permission(Permission.WRITE)
                .s3Prefix("s3://bucket2/foo/bar").build();

        GetDataAccessResult getDataAccessResponse = getDataAccessResponseSetUp("s3://bucket2/foo/bar");

        when(mockResolver.resolve(any(String.class), any(String.class))).thenReturn(TEST_S3_ACCESSGRANTS_ACCOUNT);
        when(s3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(getDataAccessResponse);
        // When
        cacheWithMockedAccountIdResolver.getCredentials(key2, TEST_S3_ACCESSGRANTS_ACCOUNT, accessDeniedCache);
        // Then
        verify(s3ControlClient, atLeastOnce()).getDataAccess(any(GetDataAccessRequest.class));

    }

    @Test
    public void accessGrantsCache_testNullS3ControlClientException() {
        assertThatThrownBy(() -> S3AccessGrantsCache.builder()
                .s3ControlClient(null)
                .maxCacheSize(DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE).build())
                .isInstanceOf(IllegalArgumentException.class);

    }

    @Test
    public void accessGrantsCache_throwsS3ControlException() {

        CacheKey key1 = CacheKey.builder()
                .credentials(S3_ACCESS_GRANTS_CREDENTIALS)
                .permission(Permission.WRITE)
                .s3Prefix("s3://bucket2/foo/bar").build();

        AWSS3ControlException s3ControlException = Mockito.mock(AWSS3ControlException.class);

        when(mockResolver.resolve(any(String.class), any(String.class))).thenReturn(TEST_S3_ACCESSGRANTS_ACCOUNT);
        when(s3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenThrow(s3ControlException);
        when(s3ControlException.getStatusCode()).thenReturn(403);
        // When
        try {
            cacheWithMockedAccountIdResolver.getCredentials(key1, TEST_S3_ACCESSGRANTS_ACCOUNT, accessDeniedCache);
        }catch (AWSS3ControlException e){}
        // Then
        assertThat(accessDeniedCache.getValueFromCache(key1)).isInstanceOf(AWSS3ControlException.class);
    }

    @Test
    public void accessGrantsCache_testTTL() {
        // When
        Instant expiration = Instant.now().plus(10, ChronoUnit.SECONDS);
        // Then
        assertThat(cacheWithMockedAccountIdResolver.getTTL(expiration)).isEqualTo(6);
    }

    @Test
    public void accessGrantsCache_testProcessingOfMatchedGrantsTarget() {
        // When
        String grant1 = "s3://bucket/foo/bar/*";
        String grant2 = "s3://bucket/foo/bar.txt";
        String grant3 = "s3://*";
        // Then
        assertThat(cache.processMatchedGrantTarget(grant1)).isEqualTo("s3://bucket/foo/bar");
        assertThat(cache.processMatchedGrantTarget(grant2)).isEqualTo("s3://bucket/foo/bar.txt");
        assertThat(cache.processMatchedGrantTarget(grant3)).isEqualTo("s3:/");
    }

    @Test
    public void accessGrantsCache_testGrantPresentForLocation() {
        // Given
        GetDataAccessResult getDataAccessResponse = getDataAccessResponseSetUp("s3://bucket/foo");
        CacheKey key1 = CacheKey.builder()
                .credentials(AWS_BASIC_CREDENTIALS)
                .permission(Permission.READ)
                .s3Prefix("s3://bucket/foo/bar/text.txt").build();

        when(mockResolver.resolve(any(String.class), any(String.class))).thenReturn(TEST_S3_ACCESSGRANTS_ACCOUNT);
        when(s3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(getDataAccessResponse);
        cacheWithMockedAccountIdResolver.getCredentials(key1, TEST_S3_ACCESSGRANTS_ACCOUNT,accessDeniedCache);
        // When
        CacheKey key2 = CacheKey.builder()
                .credentials(AWS_BASIC_CREDENTIALS)
                .permission(Permission.READ)
                .s3Prefix("s3://bucket/foo/log/text.txt").build();
        cacheWithMockedAccountIdResolver.getCredentials(key2, TEST_S3_ACCESSGRANTS_ACCOUNT,accessDeniedCache);
        // Then
        verify(s3ControlClient, times(1)).getDataAccess(any(GetDataAccessRequest.class));
    }
    @Test
    public void accessGrantsCache_testGrantWithPrefix() {
        // Given
        CacheKey key1 = CacheKey.builder()
                .credentials(AWS_BASIC_CREDENTIALS)
                .permission(Permission.READWRITE)
                .s3Prefix("s3://bucket2/foo*").build();
        CacheKey key2 = CacheKey.builder()
                .credentials(AWS_BASIC_CREDENTIALS)
                .permission(Permission.READ)
                .s3Prefix("s3://bucket2/foo/text.txt").build();
        cache.putValueInCache(key1, S3_ACCESS_GRANTS_CREDENTIALS, 2);
        // When
        AWSCredentials cacheValue1 = cache.getCredentials(key2, TEST_S3_ACCESSGRANTS_ACCOUNT, accessDeniedCache);
        // Then
        assertThat(cacheValue1).isEqualTo(S3_ACCESS_GRANTS_CREDENTIALS);
        verify(s3ControlClient, times(0)).getDataAccess(any(GetDataAccessRequest.class));
    }

    @Test
    public void accessGrantsCache_testPutValueInCacheForObjectLevelGrant() {
        // When
        Instant ttl  = Instant.now().plus(Duration.ofMinutes(1));
        Credentials creds = new Credentials().withAccessKeyId(ACCESS_KEY_ID)
                .withSecretAccessKey(SECRET_ACCESS_KEY)
                .withSessionToken(SESSION_TOKEN)
                .withExpiration(Date.from(ttl));
        GetDataAccessResult getDataAccessResponse =
                new GetDataAccessResult()
                        .withCredentials(creds)
                        .withMatchedGrantTarget("s3://bucket/foo");
        CacheKey key = CacheKey.builder()
                .credentials(AWS_BASIC_CREDENTIALS)
                .permission(Permission.READ)
                .s3Prefix("s3://bucket/foo/bar/text.txt").build();
        // When
        when(mockResolver.resolve(any(String.class), any(String.class))).thenReturn(TEST_S3_ACCESSGRANTS_ACCOUNT);
        when(s3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(getDataAccessResponse);
        cacheWithMockedAccountIdResolver.getCredentials(key, TEST_S3_ACCESSGRANTS_ACCOUNT,accessDeniedCache);
        cacheWithMockedAccountIdResolver.getCredentials(key, TEST_S3_ACCESSGRANTS_ACCOUNT,accessDeniedCache);
        // Then
        verify(s3ControlClient, times(2)).getDataAccess(any(GetDataAccessRequest.class));

    }
}
