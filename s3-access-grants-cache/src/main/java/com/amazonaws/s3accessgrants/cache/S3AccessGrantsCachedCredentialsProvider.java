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
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3control.AWSS3Control;
import com.amazonaws.services.s3control.model.AWSS3ControlException;
import com.amazonaws.services.s3control.model.Permission;

public interface S3AccessGrantsCachedCredentialsProvider {
    /**
     * @param credentials Credentials used for calling Access Grants.
     * @param permission Permission requested by the user. Can be Read, Write, or ReadWrite.
     * @param s3Prefix S3Prefix requested by the user. e.g., s3://bucket-name/path/to/helloworld.txt
     * @return Credentials from Access Grants.
     * @throws AWSS3ControlException in-case exception is cached.
     */
    AWSCredentials getDataAccess (AWSS3Control s3ControlClient, AWSCredentials credentials, Permission permission, String s3Prefix,
                                  String accountId) throws AWSS3ControlException;

    /**
     * *
     * @param s3Client used to make headBucket() call
     * @param bucketName name of the bucket to get the region for
     * @return Region the bucket is in
     */
    Regions getBucketRegion (AmazonS3 s3Client, String bucketName) ;
}
