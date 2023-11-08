package com.amazonaws.s3accessgrants.cache;

import com.amazonaws.services.s3control.AWSS3Control;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static com.amazonaws.s3accessgrants.cache.internal.S3AccessGrantsCacheConstants.DEFAULT_ACCOUNT_ID_EXPIRE_CACHE_AFTER_WRITE_SECONDS;
import static com.amazonaws.s3accessgrants.cache.internal.S3AccessGrantsCacheConstants.DEFAULT_ACCOUNT_ID_MAX_CACHE_SIZE;

public class S3AccessGrantsCachedAccountIdResolverCreationTest {
    private AWSS3Control s3ControlClient;

    @Before
    public void setup() {
        s3ControlClient = Mockito.mock(AWSS3Control.class);
    }

    @Test
    public void create_DefaultResolver_without_S3ControlAsyncClient_via_Constructor() {
        // Given
        s3ControlClient = null;
        // Then
        assertThatIllegalArgumentException().isThrownBy(() -> new S3AccessGrantsCachedAccountIdResolver(s3ControlClient));
    }

    @Test
    public void create_DefaultResolver_via_Constructor() {
        // When
        S3AccessGrantsCachedAccountIdResolver resolver = new S3AccessGrantsCachedAccountIdResolver(s3ControlClient);
        // Then
        assertThat(resolver).isNotNull();
        assertThat(resolver.s3ControlClient()).isEqualTo(s3ControlClient);
        assertThat(resolver.maxCacheSize()).isEqualTo(DEFAULT_ACCOUNT_ID_MAX_CACHE_SIZE);
        assertThat(resolver.expireCacheAfterWriteSeconds()).isEqualTo(DEFAULT_ACCOUNT_ID_EXPIRE_CACHE_AFTER_WRITE_SECONDS);
    }

    @Test
    public void create_Resolver_via_Builder() {
        assertThatIllegalArgumentException().isThrownBy(() -> S3AccessGrantsCachedAccountIdResolver
                .builder()
                .build());
    }

    @Test
    public void create_Resolver_without_S3ControlAsyncClient_via_Builder() {
        // Given
        s3ControlClient = null;
        //Then
        assertThatIllegalArgumentException().isThrownBy(() -> S3AccessGrantsCachedAccountIdResolver
                .builder()
                .s3ControlClient(s3ControlClient)
                .build());
    }

    @Test
    public void create_DefaultResolver_via_Builder() {
        // When
        S3AccessGrantsCachedAccountIdResolver resolver = S3AccessGrantsCachedAccountIdResolver
                .builder()
                .s3ControlClient(s3ControlClient)
                .build();
        // Then
        assertThat(resolver).isNotNull();
        assertThat(resolver.s3ControlClient()).isEqualTo(s3ControlClient);
        assertThat(resolver.maxCacheSize()).isEqualTo(DEFAULT_ACCOUNT_ID_MAX_CACHE_SIZE);
        assertThat(resolver.expireCacheAfterWriteSeconds()).isEqualTo(DEFAULT_ACCOUNT_ID_EXPIRE_CACHE_AFTER_WRITE_SECONDS);
    }

    @Test
    public void create_FullCustomizedResolver() {
        // Given
        int customMaxCacheSize = 2_000;
        int customExpireCacheAfterWriteSeconds = 3600;
        // When
        S3AccessGrantsCachedAccountIdResolver resolver = S3AccessGrantsCachedAccountIdResolver
                .builder()
                .s3ControlClient(s3ControlClient)
                .maxCacheSize(customMaxCacheSize)
                .expireCacheAfterWriteSeconds(customExpireCacheAfterWriteSeconds)
                .build();
        // Then
        assertThat(resolver).isNotNull();
        assertThat(resolver.s3ControlClient()).isEqualTo(s3ControlClient);
        assertThat(resolver.maxCacheSize()).isEqualTo(customMaxCacheSize);
        assertThat(resolver.expireCacheAfterWriteSeconds()).isEqualTo(customExpireCacheAfterWriteSeconds);
    }

    @Test
    public void create_CustomizedResolver_exceeds_MaxCacheSize() {
        // Given
        int customMaxCacheSize = 2_000_000;
        // Then
        assertThatIllegalArgumentException().isThrownBy(() -> S3AccessGrantsCachedAccountIdResolver
                .builder()
                .s3ControlClient(s3ControlClient)
                .maxCacheSize(customMaxCacheSize)
                .build());
    }

    @Test
    public void create_CustomizedResolver_exceeds_ExpireCacheAfterWriteSeconds() {
        // Given
        int customExpireCacheAfterWriteSeconds = 3_000_000;
        // Then
        assertThatIllegalArgumentException().isThrownBy(() -> S3AccessGrantsCachedAccountIdResolver
                .builder()
                .s3ControlClient(s3ControlClient)
                .expireCacheAfterWriteSeconds(customExpireCacheAfterWriteSeconds)
                .build());
    }

    @Test
    public void copy_Resolver() {
        // Given
        int customMaxCacheSize = 2_000;
        int customExpireCacheAfterWriteSeconds = 3600;
        // When
        S3AccessGrantsCachedAccountIdResolver resolver = S3AccessGrantsCachedAccountIdResolver
                .builder()
                .s3ControlClient(s3ControlClient)
                .maxCacheSize(customMaxCacheSize)
                .expireCacheAfterWriteSeconds(customExpireCacheAfterWriteSeconds)
                .build();
        S3AccessGrantsCachedAccountIdResolver copy = resolver.toBuilder().build();
        // Then
        assertThat(copy).isNotNull();
        assertThat(copy.s3ControlClient()).isEqualTo(s3ControlClient);
        assertThat(copy.maxCacheSize()).isEqualTo(customMaxCacheSize);
        assertThat(copy.expireCacheAfterWriteSeconds()).isEqualTo(customExpireCacheAfterWriteSeconds);
    }
}
