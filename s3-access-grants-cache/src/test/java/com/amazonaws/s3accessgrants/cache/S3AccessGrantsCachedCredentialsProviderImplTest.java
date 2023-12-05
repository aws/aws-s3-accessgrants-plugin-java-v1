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
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.s3control.AWSS3Control;
import com.amazonaws.services.s3control.model.Credentials;
import com.amazonaws.services.s3control.model.GetDataAccessRequest;
import com.amazonaws.services.s3control.model.GetDataAccessResult;
import com.amazonaws.services.s3control.model.Permission;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Date;
import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static com.amazonaws.s3accessgrants.cache.S3AccessGrantsTestConstants.ACCESS_KEY_ID;
import static com.amazonaws.s3accessgrants.cache.S3AccessGrantsTestConstants.SECRET_ACCESS_KEY;
import static com.amazonaws.s3accessgrants.cache.S3AccessGrantsTestConstants.SESSION_TOKEN;
import static com.amazonaws.s3accessgrants.cache.S3AccessGrantsTestConstants.AWS_SESSION_CREDENTIALS;
import static com.amazonaws.s3accessgrants.cache.S3AccessGrantsTestConstants.TEST_S3_ACCESSGRANTS_ACCOUNT;

public class S3AccessGrantsCachedCredentialsProviderImplTest {
    S3AccessGrantsCachedCredentialsProviderImpl cache;
    S3AccessGrantsCachedCredentialsProviderImpl cacheWithMockedAccountIdResolver;
    static AWSS3Control s3ControlClient = Mockito.mock(AWSS3Control.class);
    static S3AccessGrantsCachedAccountIdResolver mockResolver = Mockito.mock(S3AccessGrantsCachedAccountIdResolver.class);
    static Credentials credentials;


    @Before
    public void setup() {
        cache = S3AccessGrantsCachedCredentialsProviderImpl.builder()
                .s3ControlClient(s3ControlClient).build();
        cacheWithMockedAccountIdResolver = S3AccessGrantsCachedCredentialsProviderImpl.builder()
                .s3ControlClient(s3ControlClient)
                .s3AccessGrantsCachedAccountIdResolver(mockResolver)
                .buildWithAccountIdResolver();
    }

    @Before
    public void clearCache() {
        cache.invalidateCache();
    }

    public GetDataAccessResult getDataAccessResponseSetUp(String s3Prefix) {

        Instant ttl  = Instant.now().plus(Duration.ofMinutes(1));
        credentials = new Credentials().withAccessKeyId(ACCESS_KEY_ID)
                .withSecretAccessKey(SECRET_ACCESS_KEY)
                .withSessionToken(SESSION_TOKEN)
                .withExpiration(Date.from(ttl));
        return new GetDataAccessResult()
                .withCredentials(credentials)
                .withMatchedGrantTarget(s3Prefix);
    }

    @Test
    public void cacheImpl_cacheHit() {
        // Given
        GetDataAccessResult getDataAccessResponse = getDataAccessResponseSetUp("s3://bucket2/foo/bar");
        when(mockResolver.resolve(any(String.class), any(String.class))).thenReturn(TEST_S3_ACCESSGRANTS_ACCOUNT);
        when(s3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(getDataAccessResponse);
        cacheWithMockedAccountIdResolver.getDataAccess(AWS_SESSION_CREDENTIALS, Permission.READ, "s3://bucket2/foo/bar", TEST_S3_ACCESSGRANTS_ACCOUNT);
        AWSSessionCredentials sessionCredentials = new BasicSessionCredentials(credentials.getAccessKeyId(), credentials.getSecretAccessKey(), credentials.getSessionToken());
        // When
        AWSCredentials credentialsIdentity = cacheWithMockedAccountIdResolver.getDataAccess(AWS_SESSION_CREDENTIALS,
                Permission.READ,
                "s3://bucket2/foo/bar",
                TEST_S3_ACCESSGRANTS_ACCOUNT);
        // Then
        assertThat(credentialsIdentity.getAWSAccessKeyId()).isEqualTo(sessionCredentials.getAWSAccessKeyId());
        assertThat(credentialsIdentity.getAWSSecretKey()).isEqualTo(sessionCredentials.getAWSSecretKey());

    }

    @Test
    public void cacheImpl_cacheMiss() {
        // Given
        GetDataAccessResult getDataAccessResponse = getDataAccessResponseSetUp("s3://bucket2/foo/bar");
        when(mockResolver.resolve(any(String.class), any(String.class))).thenReturn(TEST_S3_ACCESSGRANTS_ACCOUNT);
        when(s3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(getDataAccessResponse);
        // When
        cacheWithMockedAccountIdResolver.getDataAccess(AWS_SESSION_CREDENTIALS, Permission.READ, "s3://bucket2/foo/bar", TEST_S3_ACCESSGRANTS_ACCOUNT);
        // Then
        verify(s3ControlClient, atLeastOnce()).getDataAccess(any(GetDataAccessRequest.class));

    }

}
