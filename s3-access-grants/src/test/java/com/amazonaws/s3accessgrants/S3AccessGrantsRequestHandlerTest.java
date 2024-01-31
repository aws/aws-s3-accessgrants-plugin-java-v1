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

package com.amazonaws.s3accessgrants;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.s3accessgrants.cache.S3AccessGrantsCachedCredentialsProviderImpl;
import com.amazonaws.s3accessgrants.internal.S3AccessGrantsStaticOperationDetails;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3control.AWSS3Control;
import com.amazonaws.services.s3control.model.Permission;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityResult;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class S3AccessGrantsRequestHandlerTest {

    public static final String ACCESS_KEY_ID = "accessKey";
    public static final String SECRET_ACCESS_KEY = "secretAccessKey";
    public static final String SESSION_TOKEN = "sessionToken";
    private final S3AccessGrantsCachedCredentialsProviderImpl cachedCredentialsProvider = Mockito.mock(S3AccessGrantsCachedCredentialsProviderImpl.class);
    AWSCredentials accessGrantsCredentials = new BasicSessionCredentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY, SESSION_TOKEN);
    BasicAWSCredentials basicAWSCredentials= new BasicAWSCredentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY);
    private final AWSCredentialsProvider credentialsProvider = Mockito.mock(AWSCredentialsProvider.class);
    private final AWSSecurityTokenService stsClient = Mockito.mock(AWSSecurityTokenService.class);
    private final AWSS3Control mockedS3ControlClient = Mockito.mock(AWSS3Control.class);
    S3AccessGrantsRequestHandler requestHandler;
    AmazonWebServiceRequest getObjectRequest;
    private final S3AccessGrantsStaticOperationDetails mockedOperationDetails = Mockito.mock(S3AccessGrantsStaticOperationDetails.class);
    private final S3AccessGrantsStaticOperationDetails operationDetails = new S3AccessGrantsStaticOperationDetails();

    @Before
    public void setup() {
        getObjectRequest = new GetObjectRequest("test-bucket", "PrefixA/file1.txt");

    }

    @Test
    public void accessGrantsRequestHandler_resolveCredentials (){
        //Given
        requestHandler = new S3AccessGrantsRequestHandler(mockedS3ControlClient, true, credentialsProvider, Regions.US_EAST_2,
                stsClient, cachedCredentialsProvider, operationDetails);
        //When
        GetCallerIdentityResult result = new GetCallerIdentityResult().withAccount("12345678910");
        when(stsClient.getCallerIdentity(any(GetCallerIdentityRequest.class))).thenReturn(result);
        when(credentialsProvider.getCredentials()).thenReturn(basicAWSCredentials);
        when(cachedCredentialsProvider.getDataAccess(any(AWSS3Control.class), any(AWSCredentials.class), any(Permission.class), any(String.class), any(String.class)))
                .thenReturn(accessGrantsCredentials);
        //Then
        assertThat(requestHandler.resolve(getObjectRequest).getCredentials()).isEqualTo(accessGrantsCredentials);
    }

    @Test(expected = AmazonServiceException.class)
    public void accessGrantsRequestHandler_throwException_fallbackDisabled (){
        //Given
        requestHandler = new S3AccessGrantsRequestHandler(mockedS3ControlClient, false, credentialsProvider, Regions.US_EAST_2,
                stsClient, cachedCredentialsProvider, mockedOperationDetails);
        //When
        GetCallerIdentityResult result = new GetCallerIdentityResult().withAccount("12345678910");
        when(mockedOperationDetails.getOperation(any(String.class))).thenReturn("UnsupportedOperation");
        when(mockedOperationDetails.getPermission(any(String.class))).thenThrow(new AmazonServiceException(""));
        when(stsClient.getCallerIdentity(any(GetCallerIdentityRequest.class))).thenReturn(result);
        when(cachedCredentialsProvider.getDataAccess(any(AWSS3Control.class), any(AWSCredentials.class), any(Permission.class), any(String.class), any(String.class)))
                .thenReturn(accessGrantsCredentials);
        //Then
        requestHandler.resolve(getObjectRequest);
    }

    @Test
    public void accessGrantsRequestHandler_fallbackToCredentialProviderCredentials (){
        //Given
        requestHandler = new S3AccessGrantsRequestHandler(mockedS3ControlClient, true, credentialsProvider, Regions.US_EAST_2,
                stsClient, cachedCredentialsProvider, mockedOperationDetails);
        //When
        GetCallerIdentityResult result = new GetCallerIdentityResult().withAccount("12345678910");
        when(mockedOperationDetails.getOperation(any(String.class))).thenReturn("UnsupportedOperation");
        when(mockedOperationDetails.getPermission(any(String.class))).thenThrow(new AmazonServiceException(""));
        when(stsClient.getCallerIdentity(any(GetCallerIdentityRequest.class))).thenReturn(result);
        when(cachedCredentialsProvider.getDataAccess(any(AWSS3Control.class), any(AWSCredentials.class), any(Permission.class), any(String.class), any(String.class)))
                .thenReturn(accessGrantsCredentials);
        //Then
        assertThat(requestHandler.resolve(getObjectRequest)).isEqualTo(credentialsProvider);

    }

    @Test
    public void accessGrantsRequestHandler_unsupportedOperation_fallbackDisabled (){
        //Given
        requestHandler = new S3AccessGrantsRequestHandler(mockedS3ControlClient, false, credentialsProvider, Regions.US_EAST_2,
                stsClient, cachedCredentialsProvider, mockedOperationDetails);
        //When
        GetCallerIdentityResult result = new GetCallerIdentityResult().withAccount("12345678910");
        when(mockedOperationDetails.getOperation(any(String.class))).thenReturn("UnsupportedOperation");
        when(mockedOperationDetails.getPermission(any(String.class))).thenThrow(new AmazonServiceException("", new UnsupportedOperationException("")));
        when(stsClient.getCallerIdentity(any(GetCallerIdentityRequest.class))).thenReturn(result);
        when(cachedCredentialsProvider.getDataAccess(any(AWSS3Control.class), any(AWSCredentials.class), any(Permission.class), any(String.class), any(String.class)))
                .thenReturn(accessGrantsCredentials);
        //Then
        assertThat(requestHandler.resolve(getObjectRequest)).isEqualTo(credentialsProvider);

    }

    @Test(expected = NullPointerException.class)
    public void accessGrantsRequestHandler_testNullAccountId () {
        //Given
        GetCallerIdentityResult result = new GetCallerIdentityResult();
        //When
        when(stsClient.getCallerIdentity(any(GetCallerIdentityRequest.class))).thenReturn(result);
        //Then
        requestHandler.getCallerAccountId();
    }


}
