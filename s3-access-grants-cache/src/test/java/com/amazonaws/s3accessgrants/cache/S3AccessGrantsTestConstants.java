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

import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;

public class S3AccessGrantsTestConstants {
    public static final String TEST_S3_BUCKET = "bucket-name";
    public static final String TEST_S3_PREFIX = "s3://" + TEST_S3_BUCKET + "/path/to/helloworld.txt";
    public static final String TEST_S3_PREFIX_2 = "s3://" + TEST_S3_BUCKET + "/path/to/helloworld2.txt";
    public static final String TEST_S3_ACCESSGRANTS_ACCOUNT = "123456789012";
    public static final String TEST_S3_ACCESSGRANTS_INSTANCE_DEFAULT = "default";
    public static final String TEST_S3_ACCESSGRANTS_INSTANCE_ARN = "arn:aws:s3:us-east-2:"
            + TEST_S3_ACCESSGRANTS_ACCOUNT + ":access-grants/"
            + TEST_S3_ACCESSGRANTS_INSTANCE_DEFAULT;

    public static final String ACCESS_KEY_ID = "accessKey";
    public static final String SECRET_ACCESS_KEY = "secretAccessKey";
    public static final String SESSION_TOKEN = "sessionToken";

    public static final BasicAWSCredentials AWS_BASIC_CREDENTIALS = new BasicAWSCredentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY);
    public static final AWSSessionCredentials AWS_SESSION_CREDENTIALS =
            new BasicSessionCredentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY, SESSION_TOKEN);
    public static final AWSSessionCredentials S3_ACCESS_GRANTS_CREDENTIALS =
            new BasicSessionCredentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY, SESSION_TOKEN);
}
