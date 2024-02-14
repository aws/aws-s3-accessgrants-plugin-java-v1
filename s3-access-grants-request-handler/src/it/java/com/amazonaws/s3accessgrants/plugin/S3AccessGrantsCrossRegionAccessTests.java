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

package com.amazonaws.s3accessgrants.plugin;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.regions.Regions;
import com.amazonaws.s3accessgrants.cache.S3AccessGrantsCachedCredentialsProviderImpl;
import com.amazonaws.s3accessgrants.plugin.internal.S3AccessGrantsStaticOperationDetails;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3control.AWSS3Control;
import com.amazonaws.services.s3control.AWSS3ControlClientBuilder;
import com.amazonaws.services.s3control.model.Permission;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class S3AccessGrantsCrossRegionAccessTests {
    static ProfileCredentialsProvider profileCredentialsProvider;
    static AmazonS3 s3Client;
    static S3AccessGrantsRequestHandler requestHandler;
    static S3AccessGrantsCachedCredentialsProviderImpl cachedCredentialsProvider;
    static AWSS3Control awsS3ControlClient;
    static AWSSecurityTokenService stsClient;
    static S3AccessGrantsStaticOperationDetails operationDetails;
    private String newBucketName = "access-grants-sdk-v1-test-bucket-test1";

    @BeforeClass
    public static void setup() throws IOException, InterruptedException {
        S3AccessGrantsIntegrationTestSetUpUtils.setUpAccessGrantsInstanceForTests();
        profileCredentialsProvider = new ProfileCredentialsProvider(S3AccessGrantsIntegrationTestUtils.TEST_CREDENTIALS_PROFILE_NAME);

    }

    public static void createS3ClientInDifferentRegion(boolean fallback, boolean crossRegionAccess){

        requestHandler = spy(S3AccessGrantsRequestHandler.builder().enableFallback(fallback).enableCrossRegionAccess(crossRegionAccess)
                .region(Regions.US_WEST_1).credentialsProvider(profileCredentialsProvider).build());

        s3Client = AmazonS3Client.builder().withRequestHandlers(new RequestHandler2() {
                    @Override
                    public AmazonWebServiceRequest beforeExecution(AmazonWebServiceRequest request) {

                        AWSCredentialsProvider creds = requestHandler.resolve(request);
                        request.setRequestCredentialsProvider(creds);
                        return super.beforeExecution(request);
                    }
                }).withRegion(Regions.US_WEST_1)
                .withCredentials(profileCredentialsProvider)
                .withForceGlobalBucketAccessEnabled(true)
                .build();
    }

    public static void createS3ClientWithCacheInDifferentRegion(boolean fallback, boolean crossRegionAccess){
        awsS3ControlClient = AWSS3ControlClientBuilder.standard()
                .withRegion(Regions.US_WEST_1)
                .withCredentials(profileCredentialsProvider)
                .build();
        stsClient = AWSSecurityTokenServiceClientBuilder.standard()
                .withCredentials(profileCredentialsProvider)
                .withRegion(Regions.US_EAST_2).build();
        operationDetails = new S3AccessGrantsStaticOperationDetails();
        cachedCredentialsProvider = spy(S3AccessGrantsCachedCredentialsProviderImpl.builder().build());
        requestHandler = spy(new S3AccessGrantsRequestHandler(awsS3ControlClient, fallback, crossRegionAccess, profileCredentialsProvider,Regions.US_WEST_1, stsClient, cachedCredentialsProvider, operationDetails));

        s3Client = AmazonS3Client.builder().withRequestHandlers(new RequestHandler2() {
                    @Override
                    public AmazonWebServiceRequest beforeExecution(AmazonWebServiceRequest request) {
                        AWSCredentialsProvider creds = requestHandler.resolve(request);
                        request.setRequestCredentialsProvider(creds);
                        return super.beforeExecution(request);
                    }
                }).withRegion(Regions.US_WEST_1)
                .withCredentials(profileCredentialsProvider)
                .withForceGlobalBucketAccessEnabled(true)
                .build();

    }

    @AfterClass
    public static void tearDown() {
        if (!S3AccessGrantsIntegrationTestUtils.DISABLE_TEAR_DOWN) {
            S3AccessGrantsIntegrationTestSetUpUtils.tearDown();
        }
    }

    @Test
    public void supportedOperation_fallbackEnabled() {
        //Given
        createS3ClientInDifferentRegion(true, true);
        //When
        s3Client.getObject(S3AccessGrantsIntegrationTestUtils.TEST_BUCKET_NAME, S3AccessGrantsIntegrationTestUtils.TEST_OBJECT1);
        //Then
        verify(requestHandler, times(1)).resolve(any(AmazonWebServiceRequest.class));
        verify(requestHandler, times(1)).getCredentialsFromAccessGrants(any(AWSS3Control.class), any(Permission.class), any(String.class), any(String.class));
    }

    @Test
    public void supportedOperation_fallbackEnabled_crossRegionDisabled() {
        //Given
        createS3ClientInDifferentRegion(true, false);
        //When
        s3Client.getObject(S3AccessGrantsIntegrationTestUtils.TEST_BUCKET_NAME, S3AccessGrantsIntegrationTestUtils.TEST_OBJECT1);
        //Then
        verify(requestHandler, times(2)).resolve(any(AmazonWebServiceRequest.class));
        verify(requestHandler, times(1)).getCredentialsFromAccessGrants(any(AWSS3Control.class), any(Permission.class), any(String.class), any(String.class));
    }

    @Test(expected = com.amazonaws.services.s3control.model.AWSS3ControlException.class)
    public void supportedOperation_fallbackDisabled_crossRegionDisabled() {
        //Given
        createS3ClientInDifferentRegion(false, false);
        //When
        s3Client.getObject(S3AccessGrantsIntegrationTestUtils.TEST_BUCKET_NAME, S3AccessGrantsIntegrationTestUtils.TEST_OBJECT1);
    }

    @Test
    public void unsupportedOperation_fallbackEnabled() {
        //Given
        createS3ClientInDifferentRegion(true, true);
        //When
        s3Client.createBucket(newBucketName);
        //Then
        verify(requestHandler, times(1)).resolve(any(AmazonWebServiceRequest.class));
        verify(requestHandler, times(1)).shouldFallbackToDefaultCredentialsForThisCase(any(Throwable.class));
        //Clean up
        s3Client.deleteBucket(newBucketName);
    }

    @Test
    public void unsupportedOperation_fallbackEnabled_crossRegionDisabled() {
        //Given
        createS3ClientInDifferentRegion(true, false);
        //When
        s3Client.createBucket(newBucketName);
        //Then
        verify(requestHandler, times(1)).resolve(any(AmazonWebServiceRequest.class));
        verify(requestHandler, times(1)).shouldFallbackToDefaultCredentialsForThisCase(any(Throwable.class));
        //Clean up
        s3Client.deleteBucket(newBucketName);
    }

    @Test
    public void unsupportedOperation_fallbackDisabled() {
        //Given
        createS3ClientInDifferentRegion(false, true);
        //When
        s3Client.createBucket(newBucketName);
        //Then
        verify(requestHandler, times(1)).resolve(any(AmazonWebServiceRequest.class));
        verify(requestHandler, times(1)).shouldFallbackToDefaultCredentialsForThisCase(any(Throwable.class));
        //Clean up
        s3Client.deleteBucket(newBucketName);
    }

    @Test
    public void unsupportedOperation_fallbackDisabled_crossRegionDisabled() {
        //Given
        createS3ClientInDifferentRegion(false, false);
        //When
        s3Client.createBucket(newBucketName);
        //Then
        verify(requestHandler, times(1)).resolve(any(AmazonWebServiceRequest.class));
        verify(requestHandler, times(1)).shouldFallbackToDefaultCredentialsForThisCase(any(Throwable.class));
        //Clean up
        s3Client.deleteBucket(newBucketName);
    }

    @Test
    public void grantAbsent_fallbackEnabled() {
        //Given
        createS3ClientInDifferentRegion(true, true);
        //When
        s3Client.getObject(S3AccessGrantsIntegrationTestUtils.TEST_BUCKET_NAME_NOT_REGISTERED, S3AccessGrantsIntegrationTestUtils.TEST_OBJECT1);
        //Then
        verify(requestHandler, times(1)).resolve(any(AmazonWebServiceRequest.class));
        verify(requestHandler, times(1)).shouldFallbackToDefaultCredentialsForThisCase(null);
    }

    @Test
    public void grantAbsent_fallbackEnabled_crossRegionDisabled() {
        //Given
        createS3ClientInDifferentRegion(true, false);
        //When
        s3Client.getObject(S3AccessGrantsIntegrationTestUtils.TEST_BUCKET_NAME_NOT_REGISTERED, S3AccessGrantsIntegrationTestUtils.TEST_OBJECT1);
        //Then
        verify(requestHandler, times(1)).resolve(any(AmazonWebServiceRequest.class));
        verify(requestHandler, times(1)).shouldFallbackToDefaultCredentialsForThisCase(null);
    }

    @Test(expected = com.amazonaws.services.s3control.model.AWSS3ControlException.class)
    public void grantAbsent_fallbackDisabled() {
        //Given
        createS3ClientInDifferentRegion(false, true);
        //When
        S3AccessGrantsIntegrationTestUtils.putObject(s3Client, S3AccessGrantsIntegrationTestUtils.TEST_BUCKET_NAME_NOT_REGISTERED,
                S3AccessGrantsIntegrationTestUtils.TEST_OBJECT2,
                S3AccessGrantsIntegrationTestUtils.TEST_OBJECT_2_CONTENTS);
    }

    @Test(expected = com.amazonaws.services.s3control.model.AWSS3ControlException.class)
    public void grantAbsent_fallbackDisabled_crossRegionDisabled() {
        //Given
        createS3ClientInDifferentRegion(false, false);
        //When
        S3AccessGrantsIntegrationTestUtils.putObject(s3Client, S3AccessGrantsIntegrationTestUtils.TEST_BUCKET_NAME_NOT_REGISTERED,
                S3AccessGrantsIntegrationTestUtils.TEST_OBJECT2,
                S3AccessGrantsIntegrationTestUtils.TEST_OBJECT_2_CONTENTS);
    }

    @Test
    public void cacheTest_supportedOperation() {
        //Given
        createS3ClientWithCacheInDifferentRegion(true, true);
        //When
        s3Client.getObject(S3AccessGrantsIntegrationTestUtils.TEST_BUCKET_NAME, S3AccessGrantsIntegrationTestUtils.TEST_OBJECT1);
        //Then
        verify(requestHandler, times(1)).resolve(any(AmazonWebServiceRequest.class));
        verify(requestHandler, times(1)).getCredentialsFromAccessGrants(any(AWSS3Control.class), any(Permission.class), any(String.class), any(String.class));
        verify(cachedCredentialsProvider, times(1)).getDataAccess(any(AWSS3Control.class), any(AWSCredentials.class),any(Permission.class), any(String.class), any(String.class));
    }

    @Test(expected = com.amazonaws.services.s3control.model.AWSS3ControlException.class)
    public void cacheTest_supportedOperation_fallbackDisabled_crossRegionDisabled() {
        //Given
        createS3ClientWithCacheInDifferentRegion(false, false);
        //When
        s3Client.getObject(S3AccessGrantsIntegrationTestUtils.TEST_BUCKET_NAME, S3AccessGrantsIntegrationTestUtils.TEST_OBJECT1);
    }

    @Test
    public void cacheTest_supportedOperation_crossRegionDisabled() {
        //Given
        createS3ClientWithCacheInDifferentRegion(true, false);
        //When
        s3Client.getObject(S3AccessGrantsIntegrationTestUtils.TEST_BUCKET_NAME, S3AccessGrantsIntegrationTestUtils.TEST_OBJECT1);
        //Then
        verify(requestHandler, times(1)).getCredentialsFromAccessGrants(any(AWSS3Control.class), any(Permission.class), any(String.class), any(String.class));
        verify(cachedCredentialsProvider, times(1)).getDataAccess(any(AWSS3Control.class), any(AWSCredentials.class),any(Permission.class), any(String.class), any(String.class));
    }
}
