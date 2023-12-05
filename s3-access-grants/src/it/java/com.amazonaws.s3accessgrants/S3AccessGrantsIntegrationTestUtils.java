package com.amazonaws.s3accessgrants;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.amazonaws.services.identitymanagement.model.AttachRolePolicyRequest;
import com.amazonaws.services.identitymanagement.model.CreatePolicyRequest;
import com.amazonaws.services.identitymanagement.model.CreateRoleRequest;
import com.amazonaws.services.identitymanagement.model.DeletePolicyRequest;
import com.amazonaws.services.identitymanagement.model.DeleteRoleRequest;
import com.amazonaws.services.identitymanagement.model.DetachRolePolicyRequest;
import com.amazonaws.services.identitymanagement.model.EntityAlreadyExistsException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3control.AWSS3Control;
import com.amazonaws.services.s3control.AWSS3ControlClientBuilder;
import com.amazonaws.services.s3control.model.AWSS3ControlException;
import com.amazonaws.services.s3control.model.AccessGrantsLocationConfiguration;
import com.amazonaws.services.s3control.model.CreateAccessGrantRequest;
import com.amazonaws.services.s3control.model.CreateAccessGrantsInstanceRequest;
import com.amazonaws.services.s3control.model.CreateAccessGrantsInstanceResult;
import com.amazonaws.services.s3control.model.CreateAccessGrantsLocationRequest;
import com.amazonaws.services.s3control.model.DeleteAccessGrantRequest;
import com.amazonaws.services.s3control.model.DeleteAccessGrantsInstanceRequest;
import com.amazonaws.services.s3control.model.DeleteAccessGrantsLocationRequest;
import com.amazonaws.services.s3control.model.GetAccessGrantsInstanceRequest;
import com.amazonaws.services.s3control.model.GetAccessGrantsInstanceResult;
import com.amazonaws.services.s3control.model.Grantee;
import com.amazonaws.services.s3control.model.GranteeType;
import com.amazonaws.services.s3control.model.ListAccessGrantsLocationsRequest;
import com.amazonaws.services.s3control.model.ListAccessGrantsLocationsResult;
import com.amazonaws.services.s3control.model.Permission;

public class S3AccessGrantsIntegrationTestUtils {
    public static String TEST_ACCOUNT;
    public static Regions TEST_REGION = Regions.US_WEST_2;
    public static final String TEST_BUCKET_NAME = "access-grants-sdk-v1-test-bucket";
    public static final String TEST_BUCKET_NAME_NOT_REGISTERED = "access-grants-sdk-v1-test-bucket-not-registered";
    public static final String TEST_LOCATION_1 = "PrefixA/prefixB/";
    public static final String TEST_LOCATION_2 = "PrefixA/";
    public static final String TEST_OBJECT1 = TEST_LOCATION_1+"file1.txt";
    public static final String TEST_OBJECT2 = TEST_LOCATION_2+"file2.txt";
    public static final String ACCESS_GRANTS_POLICY_NAME = "access-grants-policy-sdk-test";
    public static String ACCESS_GRANTS_IAM_ROLE_NAME;
    public static final String ALLOWED_BUCKET_PREFIX = TEST_LOCATION_1+"*";
    public static final String ALLOWED_BUCKET_PREFIX2 = TEST_LOCATION_2+"*";
    public static String TEST_OBJECT_1_CONTENTS = "access grants test content in file1!";
    public static String TEST_OBJECT_2_CONTENTS = "access grants test content in file2!";
    public static String TEST_CREDENTIALS_PROFILE_NAME = "aws-test-account1";
    public static final boolean DISABLE_TEAR_DOWN = false;

    public static AmazonS3 s3clientBuilder(ProfileCredentialsProvider identityProvider, Regions region) {
        return AmazonS3Client.builder()
                .withRegion(region)
                .withCredentials(identityProvider)
                .build();
    }

    public static AWSS3Control getS3ControlClientBuilder(ProfileCredentialsProvider identityProvider, Regions region){
        return AWSS3ControlClientBuilder.standard()
                .withRegion(region)
                .withCredentials(identityProvider)
                .build();
    }

    public static String createAccessGrantsInstance(AWSS3Control s3ControlClient, String accountId){
        GetAccessGrantsInstanceRequest request = new GetAccessGrantsInstanceRequest().withAccountId(accountId);
        try {
            GetAccessGrantsInstanceResult response = s3ControlClient.getAccessGrantsInstance(request);
            return response.getAccessGrantsInstanceArn();
        } catch (AWSS3ControlException e) {
            CreateAccessGrantsInstanceRequest createRequest =  new CreateAccessGrantsInstanceRequest().withAccountId("527802564711");
            CreateAccessGrantsInstanceResult result = s3ControlClient.createAccessGrantsInstance(createRequest);
            return result.getAccessGrantsInstanceArn();
        }
    }

    public static AmazonIdentityManagement iamClientBuilder(ProfileCredentialsProvider profileCredentialsProvider, Regions region) {
        return AmazonIdentityManagementClient.builder().withCredentials(profileCredentialsProvider).withRegion(region).build();
    }

    public static String createS3AccessGrantsIAMPolicy(AmazonIdentityManagement iamClient, String policyName, String policyStatement) {
        try {
            CreatePolicyRequest createPolicyRequest = new
                    CreatePolicyRequest().withPolicyDocument(policyStatement).withPolicyName(policyName);
            return iamClient.createPolicy(createPolicyRequest).getPolicy().getArn();
        } catch (EntityAlreadyExistsException e){
            return "arn:aws:iam::" + TEST_ACCOUNT + ":policy/" + policyName;
        }
    }

    public static String createS3AccessGrantsIAMRole(AmazonIdentityManagement iamClient, String roleName, String trustPolicy) {
        try {
            CreateRoleRequest request = new
                    CreateRoleRequest().withRoleName(roleName).withAssumeRolePolicyDocument(trustPolicy);

            return iamClient.createRole(request).getRole().getArn();
        } catch (EntityAlreadyExistsException e) {
            return "arn:aws:iam::" + TEST_ACCOUNT + ":role/" + roleName;
        }
    }

    public static void attachPolicyToRole(AmazonIdentityManagement iamClient, String roleName, String policyArn) {
        AttachRolePolicyRequest rolePolicyRequest = new AttachRolePolicyRequest()
                .withRoleName(roleName)
                .withPolicyArn(policyArn);
        iamClient.attachRolePolicy(rolePolicyRequest);
    }

    public static void createBucket(AmazonS3 s3Client, String bucketName) {
        CreateBucketRequest createBucketRequest = new CreateBucketRequest(bucketName);
        s3Client.createBucket(createBucketRequest);
    }

    public static String createS3AccessGrantsLocation(AWSS3Control s3ControlClient, String s3Prefix, String accountId,
                                                      String iamRoleArn) {
        try {
            CreateAccessGrantsLocationRequest request = new CreateAccessGrantsLocationRequest().withAccountId(accountId)
                    .withLocationScope(s3Prefix).withIAMRoleArn(iamRoleArn);
            return s3ControlClient.createAccessGrantsLocation(request).getAccessGrantsLocationId();
        }catch (AWSS3ControlException e) {
            ListAccessGrantsLocationsRequest listAccessGrantsLocationsRequest = new ListAccessGrantsLocationsRequest()
                    .withAccountId(accountId).withLocationScope(s3Prefix);
            ListAccessGrantsLocationsResult result = s3ControlClient.listAccessGrantsLocations(listAccessGrantsLocationsRequest);
            System.out.println(result);

            return result.getAccessGrantsLocationsList().get(0).getAccessGrantsLocationId();
        }
    }

    public static String registerAccessGrant(AWSS3Control s3ControlClient, String s3prefix, Permission permission,
                                             String iamRoleArn,
                                             String accountId,
                                             String accessGrantsInstanceLocationId) {
        try {
            Grantee grantee = new Grantee().withGranteeType(GranteeType.IAM)
                    .withGranteeIdentifier(iamRoleArn);
            AccessGrantsLocationConfiguration accessGrantsLocationConfiguration = new AccessGrantsLocationConfiguration().withS3SubPrefix(s3prefix);
            CreateAccessGrantRequest accessGrantRequest = new CreateAccessGrantRequest()
                    .withAccessGrantsLocationId(accessGrantsInstanceLocationId)
                    .withAccountId(accountId)
                    .withGrantee(grantee)
                    .withPermission(permission)
                    .withAccessGrantsLocationConfiguration(accessGrantsLocationConfiguration);
            return s3ControlClient.createAccessGrant(accessGrantRequest).getAccessGrantId();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw e;
        }
    }

    public static PutObjectResult putObject(AmazonS3 s3Client, String bucketName, String key, String content) {
        try {
            return s3Client.putObject(bucketName, key, content);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw e;
        }
    }

    public static void deleteAccessGrant(AWSS3Control s3ControlClient, String accessGrantId,
                                         String accountId) {
        try {
            System.out.println("deleting access grants id "+ accessGrantId);
            DeleteAccessGrantRequest deleteAccessGrantRequest = new
                    DeleteAccessGrantRequest().withAccessGrantId(accessGrantId).withAccountId(accountId);
            s3ControlClient.deleteAccessGrant(deleteAccessGrantRequest);
            System.out.println("successfully deleted the access grants during test teardown!");
        } catch (Exception e) {
            System.out.println("Access Grants cannot be deleted during test setup teardown! "+ e.getMessage());
        }
    }

    public static void deleteAccessGrantLocation(AWSS3Control s3ControlClient, String accessGrantsInstanceLocationId) {
        try {
            DeleteAccessGrantsLocationRequest deleteAccessGrantsLocationRequest = new
                    DeleteAccessGrantsLocationRequest().withAccessGrantsLocationId(accessGrantsInstanceLocationId)
                    .withAccountId(S3AccessGrantsIntegrationTestUtils.TEST_ACCOUNT);
            s3ControlClient.deleteAccessGrantsLocation(deleteAccessGrantsLocationRequest);
            System.out.println("successfully deleted the access grants location during test teardown!");
        } catch (Exception e) {
            System.out.println("Access Grants Location cannot be deleted during test setup teardown! "+ e.getMessage());
        }
    }

    public static void deleteAccessGrantsInstance(AWSS3Control s3ControlClient,
                                                  String accountId) {
        try {
            DeleteAccessGrantsInstanceRequest deleteAccessGrantsInstanceRequest = new
                    DeleteAccessGrantsInstanceRequest().withAccountId(accountId);
            s3ControlClient.deleteAccessGrantsInstance(deleteAccessGrantsInstanceRequest);
            System.out.println("successfully deleted the access grants instance during test teardown!");
        } catch (Exception e) {
            System.out.println("Access Grants Instance cannot be deleted during test setup teardown! "+ e.getMessage());
        }
    }

    public static void deleteObject(AmazonS3 s3Client, String bucketName, String bucketKey) {
        try {
            DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucketName, bucketKey);
            s3Client.deleteObject(deleteObjectRequest);
            System.out.println("successfully deleted the object " + bucketKey + " during test teardown!");
        } catch (Exception e) {
            System.out.println("Object cannot be deleted during test teardown! "+ e.getMessage());
        }
    }

    public static void deleteBucket(AmazonS3 s3Client, String bucketName) {
        try {
            DeleteBucketRequest deleteBucketRequest = new DeleteBucketRequest(bucketName);
            s3Client.deleteBucket(deleteBucketRequest);
            System.out.println("successfully deleted the bucket during test teardown!");
        } catch (Exception e) {
            System.out.println("bucket cannot be deleted during test teardown! "+ e.getMessage());
        }
    }

    public static void deletePolicy(AmazonIdentityManagement iamClient, String policyArn) {
        try {
            DeletePolicyRequest deletePolicyRequest = new DeletePolicyRequest().withPolicyArn(policyArn);

            iamClient.deletePolicy(deletePolicyRequest);
            System.out.println("successfully deleted the policy during test teardown!");
        } catch (Exception e) {
            System.out.println("Policy cannot be deleted during test teardown! "+ e.getMessage());
        }
    }

    public static void deleteRole(AmazonIdentityManagement iamClient, String roleName) {
        try {
            DeleteRoleRequest deleteRoleRequest = new
                    DeleteRoleRequest().withRoleName(roleName);
            iamClient.deleteRole(deleteRoleRequest);
            System.out.println("successfully deleted the role during test teardown!");
        } catch (Exception e) {
            System.out.println("role cannot be deleted during test teardown! "+ e.getMessage());
        }
    }

    public static void detachPolicy(AmazonIdentityManagement iamClient, String policyArn, String roleName) {
        try {
            DetachRolePolicyRequest detachRolePolicyRequest = new
                    DetachRolePolicyRequest().withRoleName(roleName)
                            .withPolicyArn(policyArn);

            iamClient.detachRolePolicy(detachRolePolicyRequest);
            System.out.println("successfully deleted the role policy during test teardown!");
        } catch (Exception e) {
            System.out.println("policy cannot be detached form the role during test teardown! "+e.getMessage());
        }
    }

}
