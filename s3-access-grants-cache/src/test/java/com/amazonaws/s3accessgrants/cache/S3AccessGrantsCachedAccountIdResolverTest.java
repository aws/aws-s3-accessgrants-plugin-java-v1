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

import com.amazonaws.services.s3control.AWSS3Control;
import com.amazonaws.services.s3control.model.AWSS3ControlException;
import com.amazonaws.services.s3control.model.GetAccessGrantsInstanceForPrefixRequest;
import com.amazonaws.services.s3control.model.GetAccessGrantsInstanceForPrefixResult;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static com.amazonaws.s3accessgrants.cache.S3AccessGrantsTestConstants.TEST_S3_ACCESSGRANTS_ACCOUNT;
import static com.amazonaws.s3accessgrants.cache.S3AccessGrantsTestConstants.TEST_S3_ACCESSGRANTS_INSTANCE_ARN;
import static com.amazonaws.s3accessgrants.cache.S3AccessGrantsTestConstants.TEST_S3_ACCESSGRANTS_INSTANCE_DEFAULT;
import static com.amazonaws.s3accessgrants.cache.S3AccessGrantsTestConstants.TEST_S3_PREFIX;
import static com.amazonaws.s3accessgrants.cache.S3AccessGrantsTestConstants.TEST_S3_PREFIX_2;

public class S3AccessGrantsCachedAccountIdResolverTest {
    private AWSS3Control s3ControlClient;
    private S3AccessGrantsAccountIdResolver resolver;

    @Before
    public void setup() {
        s3ControlClient = Mockito.mock(AWSS3Control.class);
        resolver = S3AccessGrantsCachedAccountIdResolver
                .builder()
                .build();
    }

    @Test
    public void resolver_Returns_ExpectedAccountId() throws AWSS3ControlException {
        // Given
        ArgumentCaptor<GetAccessGrantsInstanceForPrefixRequest> requestArgumentCaptor =
                ArgumentCaptor.forClass(GetAccessGrantsInstanceForPrefixRequest.class);

        GetAccessGrantsInstanceForPrefixResult response = new GetAccessGrantsInstanceForPrefixResult()
                .withAccessGrantsInstanceArn(TEST_S3_ACCESSGRANTS_INSTANCE_ARN).withAccessGrantsInstanceId(TEST_S3_ACCESSGRANTS_INSTANCE_DEFAULT);

        when(s3ControlClient.getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class)))
                .thenReturn(response);
        // When
        String accountId = resolver.resolve(s3ControlClient, TEST_S3_ACCESSGRANTS_ACCOUNT, TEST_S3_PREFIX);
        // Then
        assertThat(accountId).isEqualTo(TEST_S3_ACCESSGRANTS_ACCOUNT);
        verify(s3ControlClient, times(1)).getAccessGrantsInstanceForPrefix(requestArgumentCaptor.capture());
        assertThat(requestArgumentCaptor.getValue().getAccountId()).isEqualTo(TEST_S3_ACCESSGRANTS_ACCOUNT);
        assertThat(requestArgumentCaptor.getValue().getS3Prefix()).isEqualTo(TEST_S3_PREFIX);
    }

    @Test
    public void resolver_Returns_CachedAccountId() throws AWSS3ControlException {
        // Given
        ArgumentCaptor<GetAccessGrantsInstanceForPrefixRequest> requestArgumentCaptor =
                ArgumentCaptor.forClass(GetAccessGrantsInstanceForPrefixRequest.class);

        GetAccessGrantsInstanceForPrefixResult response = new GetAccessGrantsInstanceForPrefixResult()
                .withAccessGrantsInstanceArn(TEST_S3_ACCESSGRANTS_INSTANCE_ARN).withAccessGrantsInstanceId(TEST_S3_ACCESSGRANTS_INSTANCE_DEFAULT);
        when(s3ControlClient.getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class))).thenReturn(response);
        // When attempting to resolve same prefix back to back
        String accountId1 = resolver.resolve(s3ControlClient, TEST_S3_ACCESSGRANTS_ACCOUNT, TEST_S3_PREFIX);
        String accountId2 = resolver.resolve(s3ControlClient, TEST_S3_ACCESSGRANTS_ACCOUNT, TEST_S3_PREFIX);
        // Then
        assertThat(accountId1).isEqualTo(TEST_S3_ACCESSGRANTS_ACCOUNT);
        assertThat(accountId2).isEqualTo(TEST_S3_ACCESSGRANTS_ACCOUNT);
        // Verify that we only call service 1 time and expect the next call retrieve accountId from cache
        verify(s3ControlClient, times(1)).getAccessGrantsInstanceForPrefix(requestArgumentCaptor.capture());
    }

    @Test
    public void resolver_Returns_CachedAccountId_of_Same_Bucket() throws AWSS3ControlException {
        // Given
        ArgumentCaptor<GetAccessGrantsInstanceForPrefixRequest> requestArgumentCaptor =
                ArgumentCaptor.forClass(GetAccessGrantsInstanceForPrefixRequest.class);

        GetAccessGrantsInstanceForPrefixResult response = new GetAccessGrantsInstanceForPrefixResult()
                .withAccessGrantsInstanceArn(TEST_S3_ACCESSGRANTS_INSTANCE_ARN).withAccessGrantsInstanceId(TEST_S3_ACCESSGRANTS_INSTANCE_DEFAULT);
        when(s3ControlClient.getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class))).thenReturn(response);
        // When attempting to resolve same prefix back to back
        String accountId1 = resolver.resolve(s3ControlClient, TEST_S3_ACCESSGRANTS_ACCOUNT, TEST_S3_PREFIX);
        String accountId2 = resolver.resolve(s3ControlClient, TEST_S3_ACCESSGRANTS_ACCOUNT, TEST_S3_PREFIX_2);
        // Then
        assertThat(accountId1).isEqualTo(TEST_S3_ACCESSGRANTS_ACCOUNT);
        assertThat(accountId2).isEqualTo(TEST_S3_ACCESSGRANTS_ACCOUNT);
        // Verify that we only call service 1 time and expect the next call retrieve accountId from cache
        verify(s3ControlClient, times(1)).getAccessGrantsInstanceForPrefix(requestArgumentCaptor.capture());
    }

    @Test
    public void resolver_Rethrow_S3ControlException_On_ServiceError() {
        // When
        when(s3ControlClient.getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class)))
                .thenThrow(new AWSS3ControlException(""));
        // Then
        assertThatThrownBy(() -> resolver.resolve(s3ControlClient, TEST_S3_ACCESSGRANTS_ACCOUNT, TEST_S3_PREFIX)).isInstanceOf(AWSS3ControlException.class);

    }

    @Test
    public void resolver_Throw_S3ControlException_On_Empty_ResponseArn() {
        // Given
        GetAccessGrantsInstanceForPrefixResult response = new GetAccessGrantsInstanceForPrefixResult()
                .withAccessGrantsInstanceArn("").withAccessGrantsInstanceId(TEST_S3_ACCESSGRANTS_INSTANCE_DEFAULT);
        when(s3ControlClient.getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class))).thenReturn(response);
        // Then
        assertThatThrownBy(() -> resolver.resolve(s3ControlClient, TEST_S3_ACCESSGRANTS_ACCOUNT, TEST_S3_PREFIX)).isInstanceOf(AWSS3ControlException.class);
    }

}
