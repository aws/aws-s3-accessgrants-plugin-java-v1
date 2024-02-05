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

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.model.EntityAlreadyExistsException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3control.AWSS3Control;
import com.amazonaws.services.s3control.model.Permission;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class S3AccessGrantsIntegrationTestSetUpUtils {

    private static AmazonS3 s3Client = null;
    private static AWSS3Control s3ControlClient = null;
    private static AmazonIdentityManagement iamClient = null;
    private static String accessGrantsInstanceLocationId = null;
    private static String accessGrantsArn = null;
    private static String policyArn = null;
    private static String iamRoleArn = null;
    private static List<String> registeredAccessGrants = new ArrayList<>();
    private static final Log logger = LogFactory.getLog(S3AccessGrantsIntegrationTestSetUpUtils.class);

    public static void setUpAccessGrantsInstanceForTests() throws IOException, InterruptedException {
        String defaultPropertiesFilePath = String.join(File.separator, System.getProperty("user.dir"), "default.properties");
        Properties testProps = new Properties();
        testProps.load(new FileInputStream(defaultPropertiesFilePath));
        S3AccessGrantsIntegrationTestUtils.ACCESS_GRANTS_IAM_ROLE_NAME = testProps.getProperty("IamRoleName").trim();
        S3AccessGrantsIntegrationTestUtils.TEST_ACCOUNT = testProps.getProperty("accountId").trim();
        S3AccessGrantsIntegrationTestUtils.TEST_CREDENTIALS_PROFILE_NAME = testProps.getProperty("credentialsProfile").trim();

        ProfileCredentialsProvider profileCredentialsProvider = new ProfileCredentialsProvider(S3AccessGrantsIntegrationTestUtils.TEST_CREDENTIALS_PROFILE_NAME);

        iamClient = S3AccessGrantsIntegrationTestUtils.iamClientBuilder(profileCredentialsProvider, S3AccessGrantsIntegrationTestUtils.TEST_REGION);
        policyArn = createS3AccessGrantsIAMPolicy();
        iamRoleArn = S3AccessGrantsIntegrationTestUtils.createS3AccessGrantsIAMRole(iamClient, S3AccessGrantsIntegrationTestUtils.ACCESS_GRANTS_IAM_ROLE_NAME,
                createS3AccessGrantsIAMTrustRelationship());
        S3AccessGrantsIntegrationTestUtils.attachPolicyToRole(iamClient, S3AccessGrantsIntegrationTestUtils.ACCESS_GRANTS_IAM_ROLE_NAME, policyArn);
        s3Client = S3AccessGrantsIntegrationTestUtils.s3clientBuilder(profileCredentialsProvider, S3AccessGrantsIntegrationTestUtils.TEST_REGION);
        s3ControlClient = S3AccessGrantsIntegrationTestUtils.getS3ControlClientBuilder(profileCredentialsProvider, S3AccessGrantsIntegrationTestUtils.TEST_REGION);
        CreateAccessGrantsBucket(S3AccessGrantsIntegrationTestUtils.TEST_BUCKET_NAME);
        CreateAccessGrantsBucket(S3AccessGrantsIntegrationTestUtils.TEST_BUCKET_NAME_NOT_REGISTERED);

        Thread.sleep(5000);
        accessGrantsArn = S3AccessGrantsIntegrationTestUtils.createAccessGrantsInstance(s3ControlClient, S3AccessGrantsIntegrationTestUtils.TEST_ACCOUNT);

        accessGrantsInstanceLocationId = S3AccessGrantsIntegrationTestUtils.createS3AccessGrantsLocation(s3ControlClient,
                "s3://" + S3AccessGrantsIntegrationTestUtils.TEST_BUCKET_NAME,
                S3AccessGrantsIntegrationTestUtils.TEST_ACCOUNT,
                iamRoleArn);
        registeredAccessGrants.add(S3AccessGrantsIntegrationTestUtils.registerAccessGrant(s3ControlClient,
                S3AccessGrantsIntegrationTestUtils.ALLOWED_BUCKET_PREFIX,
                Permission.READ, iamRoleArn,
                S3AccessGrantsIntegrationTestUtils.TEST_ACCOUNT, accessGrantsInstanceLocationId));
        registeredAccessGrants.add(S3AccessGrantsIntegrationTestUtils.
                registerAccessGrant(s3ControlClient, S3AccessGrantsIntegrationTestUtils.ALLOWED_BUCKET_PREFIX,
                        Permission.WRITE, iamRoleArn,
                        S3AccessGrantsIntegrationTestUtils.TEST_ACCOUNT, accessGrantsInstanceLocationId));
        registeredAccessGrants.add(S3AccessGrantsIntegrationTestUtils.registerAccessGrant(s3ControlClient,
                S3AccessGrantsIntegrationTestUtils.ALLOWED_BUCKET_PREFIX2, Permission.WRITE, iamRoleArn, S3AccessGrantsIntegrationTestUtils.TEST_ACCOUNT, accessGrantsInstanceLocationId));

        S3AccessGrantsIntegrationTestUtils.putObject(s3Client, S3AccessGrantsIntegrationTestUtils.TEST_BUCKET_NAME,
                S3AccessGrantsIntegrationTestUtils.TEST_OBJECT1,
                S3AccessGrantsIntegrationTestUtils.TEST_OBJECT_1_CONTENTS);
        S3AccessGrantsIntegrationTestUtils.putObject(s3Client, S3AccessGrantsIntegrationTestUtils.TEST_BUCKET_NAME,
                S3AccessGrantsIntegrationTestUtils.TEST_OBJECT2,
                S3AccessGrantsIntegrationTestUtils.TEST_OBJECT_2_CONTENTS);
        S3AccessGrantsIntegrationTestUtils.putObject(s3Client, S3AccessGrantsIntegrationTestUtils.TEST_BUCKET_NAME_NOT_REGISTERED,
                S3AccessGrantsIntegrationTestUtils.TEST_OBJECT1,
                S3AccessGrantsIntegrationTestUtils.TEST_OBJECT_1_CONTENTS);

    }

    public static String createS3AccessGrantsIAMPolicy() {

        String policyStatement = "{\n"
                + "   \"Version\":\"2012-10-17\",\n"
                + "   \"Statement\":[\n"
                + "      {\n"
                + "         \"Effect\":\"Allow\",\n"
                + "         \"Action\":[\n"
                + "            \"s3:PutObject\",\n"
                + "            \"s3:GetObject\",\n"
                + "            \"s3:DeleteObject\"\n"
                + "         ],\n"
                + "         \"Resource\":[\n"
                + "            \"arn:aws:s3:::"+S3AccessGrantsIntegrationTestUtils.TEST_BUCKET_NAME+"/*\"\n"
                + "         ]\n"
                + "      },\n"
                + "      {\n"
                + "         \"Effect\":\"Allow\",\n"
                + "         \"Action\":[\n"
                + "            \"s3:ListBucket\"\n"
                + "         ],\n"
                + "         \"Resource\":[\n"
                + "            \"arn:aws:s3:::"+S3AccessGrantsIntegrationTestUtils.TEST_BUCKET_NAME+"\""
                + "         ]\n"
                + "      },\n"
                + "       {\n"
                + "         \"Effect\":\"Allow\",\n"
                + "         \"Action\":[\n"
                + "            \"*\""
                + "         ],\n"
                + "         \"Resource\":[\n"
                + "            \"*\""
                + "         ]\n"
                + "      }"
                + "   ]\n"
                + "}";

        try {
            return S3AccessGrantsIntegrationTestUtils.createS3AccessGrantsIAMPolicy(iamClient,
                S3AccessGrantsIntegrationTestUtils.ACCESS_GRANTS_POLICY_NAME,
                policyStatement);
        } catch (EntityAlreadyExistsException e){
            return "arn:aws:iam::" + S3AccessGrantsIntegrationTestUtils.TEST_ACCOUNT + ":policy/" + S3AccessGrantsIntegrationTestUtils.ACCESS_GRANTS_POLICY_NAME;
        }

    }

    public static String createS3AccessGrantsIAMTrustRelationship() {
        return "{\n"
                + "  \"Version\": \"2012-10-17\",\n"
                + "  \"Statement\": [\n"
                + "    {\n"
                + "      \"Sid\": \"Stmt1685556427189\",\n"
                + "      \"Action\": [\"sts:AssumeRole\"],\n"
                + "      \"Effect\": \"Allow\",\n"
                + "      \"Principal\": {\"Service\": \"access-grants.s3.amazonaws.com\"}\n"
                + "    },\n"
                + "    {\n"
                + "      \"Action\": [\"sts:AssumeRole\"],\n"
                + "      \"Effect\": \"Allow\",\n"
                + "      \"Principal\": {\"AWS\": \""+S3AccessGrantsIntegrationTestUtils.TEST_ACCOUNT+"\"}\n"
                + "    }\n"
                + "  ]\n"
                + "}";
    }

    public static void CreateAccessGrantsBucket(String bucketName) {
        try {
            S3AccessGrantsIntegrationTestUtils.createBucket(s3Client, bucketName);
            logger.info("Created the bucket " + bucketName + " in region " + S3AccessGrantsIntegrationTestUtils.TEST_REGION);
        } catch (Exception e) {
            logger.info(e.getMessage());
        }
    }

    public static void tearDown() {
        registeredAccessGrants.forEach(accessGrantId -> {
            S3AccessGrantsIntegrationTestUtils.deleteAccessGrant( s3ControlClient, accessGrantId,
                    S3AccessGrantsIntegrationTestUtils.TEST_ACCOUNT);
        });
        S3AccessGrantsIntegrationTestUtils.deleteAccessGrantLocation(s3ControlClient, accessGrantsInstanceLocationId);
        S3AccessGrantsIntegrationTestUtils.deleteAccessGrantsInstance(s3ControlClient,
                S3AccessGrantsIntegrationTestUtils.TEST_ACCOUNT );
        S3AccessGrantsIntegrationTestUtils.deleteObject(s3Client, S3AccessGrantsIntegrationTestUtils.TEST_BUCKET_NAME,
                S3AccessGrantsIntegrationTestUtils.TEST_OBJECT1);
        S3AccessGrantsIntegrationTestUtils.deleteObject(s3Client, S3AccessGrantsIntegrationTestUtils.TEST_BUCKET_NAME,
                S3AccessGrantsIntegrationTestUtils.TEST_OBJECT2);
        S3AccessGrantsIntegrationTestUtils.deleteBucket(s3Client, S3AccessGrantsIntegrationTestUtils.TEST_BUCKET_NAME);
        S3AccessGrantsIntegrationTestUtils.deleteObject(s3Client, S3AccessGrantsIntegrationTestUtils.TEST_BUCKET_NAME_NOT_REGISTERED,
                S3AccessGrantsIntegrationTestUtils.TEST_OBJECT1);
        S3AccessGrantsIntegrationTestUtils.deleteBucket(s3Client, S3AccessGrantsIntegrationTestUtils.TEST_BUCKET_NAME_NOT_REGISTERED);
    }

}
