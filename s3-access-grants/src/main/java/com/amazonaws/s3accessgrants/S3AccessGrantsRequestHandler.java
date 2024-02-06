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
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.s3accessgrants.cache.S3AccessGrantsCachedCredentialsProviderImpl;
import com.amazonaws.s3accessgrants.internal.S3AccessGrantsStaticOperationDetails;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3control.AWSS3Control;
import com.amazonaws.services.s3control.AWSS3ControlClientBuilder;
import com.amazonaws.services.s3control.model.Permission;
import com.amazonaws.services.s3control.model.Privilege;
import com.amazonaws.s3accessgrants.internal.S3AccessGrantsUtils;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.assertj.core.util.VisibleForTesting;

import java.util.concurrent.ConcurrentHashMap;

public class S3AccessGrantsRequestHandler {
    private final boolean enableFallback;
    private final Privilege privilege;
    private final int duration;
    private final AWSCredentialsProvider credentialsProvider;
    private S3AccessGrantsStaticOperationDetails operationDetails = new S3AccessGrantsStaticOperationDetails();
    private AWSSecurityTokenService stsClient;
    private final Regions region;
    private AWSS3Control awsS3ControlClient;
    private S3AccessGrantsCachedCredentialsProviderImpl cacheImpl;
    private static final Log logger = LogFactory.getLog(S3AccessGrantsRequestHandler.class);
    private final boolean enableCrossRegionAccess;
    private ConcurrentHashMap<Regions, AWSS3Control> clientsCache = new ConcurrentHashMap<>();

    private S3AccessGrantsRequestHandler(boolean enableFallback, Privilege privilege, int duration, AWSCredentialsProvider credentialsProvider, Regions region, Boolean enableCrossRegionAccess) {
        this.enableFallback = enableFallback;
        this.privilege = privilege;
        this.duration = duration;
        this.credentialsProvider = credentialsProvider;
        this.region = region;
        this.stsClient = AWSSecurityTokenServiceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(Regions.US_EAST_2).build();
        this.cacheImpl = S3AccessGrantsCachedCredentialsProviderImpl.builder()
                .duration(duration).build();
        this.enableCrossRegionAccess = enableCrossRegionAccess;
    }

    @VisibleForTesting
    S3AccessGrantsRequestHandler(AWSS3Control awsS3ControlClient, boolean enableFallback, boolean enableCrossRegionAccess, AWSCredentialsProvider credentialsProvider, Regions region, AWSSecurityTokenService stsClient, S3AccessGrantsCachedCredentialsProviderImpl cacheImpl, S3AccessGrantsStaticOperationDetails operationDetails) {
        this.enableFallback = enableFallback;
        this.privilege = Privilege.Default;
        this.duration = 3600;
        this.credentialsProvider = credentialsProvider;
        this.region = region;
        this.stsClient = stsClient;
        this.awsS3ControlClient = awsS3ControlClient;
        this.cacheImpl = cacheImpl;
        this.operationDetails = operationDetails;
        this.enableCrossRegionAccess = enableCrossRegionAccess;
    }

    @VisibleForTesting
    S3AccessGrantsRequestHandler(AWSS3Control awsS3ControlClient, boolean enableFallback, boolean enableCrossRegionAccess, AWSCredentialsProvider credentialsProvider, Regions region, AWSSecurityTokenService stsClient, S3AccessGrantsCachedCredentialsProviderImpl cacheImpl, S3AccessGrantsStaticOperationDetails operationDetails, ConcurrentHashMap<Regions, AWSS3Control> clientsCache) {
        this(awsS3ControlClient,enableFallback, enableCrossRegionAccess, credentialsProvider, region, stsClient, cacheImpl, operationDetails);
        this.clientsCache = clientsCache;
    }

    public static S3AccessGrantsRequestHandler.Builder builder() {
        return new S3AccessGrantsRequestHandler.BuilderImpl();
    }

    public interface Builder {
        S3AccessGrantsRequestHandler build();
        S3AccessGrantsRequestHandler.Builder enableFallback(boolean enableFallback);
        S3AccessGrantsRequestHandler.Builder enableCrossRegionAccess(boolean enableCrossRegionAccess);
        S3AccessGrantsRequestHandler.Builder privilege(Privilege privilege);
        S3AccessGrantsRequestHandler.Builder duration(int duration);
        S3AccessGrantsRequestHandler.Builder credentialsProvider(AWSCredentialsProvider credentialsProvider);
        S3AccessGrantsRequestHandler.Builder region(Regions region);
    }

    static final class BuilderImpl implements S3AccessGrantsRequestHandler.Builder {
        private boolean enableFallback = S3AccessGrantsUtils.DEFAULT_FALLBACK;
        private boolean enableCrossRegionAccess = S3AccessGrantsUtils.DEFAULT_CROSS_REGION_ACCESS;
        private Privilege privilege = Privilege.Default;
        private int duration = S3AccessGrantsUtils.DEFAULT_DURATION;
        private AWSCredentialsProvider credentialsProvider;
        private Regions region;

        @Override
        public S3AccessGrantsRequestHandler build() {
            return new S3AccessGrantsRequestHandler(enableFallback,privilege,duration, credentialsProvider, region, enableCrossRegionAccess);
        }

        @Override
        public Builder enableFallback(boolean enableFallback) {
            this.enableFallback = enableFallback;
            return this;
        }

        @Override
        public Builder enableCrossRegionAccess(boolean enableCrossRegionAccess) {
            this.enableCrossRegionAccess = enableCrossRegionAccess;
            return this;
        }

        @Override
        public Builder privilege(Privilege privilege) {
            this.privilege = privilege;
            return this;
        }

        @Override
        public Builder duration(int duration) {
            this.duration = duration;
            return this;
        }

        @Override
        public Builder credentialsProvider(AWSCredentialsProvider credentialsProvider) {
            this.credentialsProvider = credentialsProvider;
            return this;
        }

        @Override
        public Builder region(Regions region) {
            this.region = region;
            return this;
        }
    }

    /**
     * this method fetches credentials from Access Grants
     * @param request S3 request for which we override the credentials
     * @return credentials from Access Grants
     */

    public AWSCredentialsProvider resolve (AmazonWebServiceRequest request) {
        AWSS3Control awsS3ControlClient;
        try {
            String s3Prefix = operationDetails.getPath(request);
            String operation = operationDetails.getOperation(request.getClass().toString());
            Permission permission = operationDetails.getPermission(operation);

            if (enableCrossRegionAccess) {
                logger.info("Cross region access enabled.");
                AmazonS3 s3Client = AmazonS3Client.builder().withRegion(region)
                        .withCredentials(credentialsProvider)
                        .withForceGlobalBucketAccessEnabled(true)
                        .build();

                awsS3ControlClient = getS3ControlClientForRegion(s3Client, s3Prefix);
            }
            else {
                awsS3ControlClient = AWSS3ControlClientBuilder.standard()
                        .withRegion(region)
                        .withCredentials(credentialsProvider)
                        .build();

            }
            logger.debug(" Calling S3 Access Grants with the following request params! ");
            logger.debug("operation : " + operation);
            logger.debug(" S3Prefix : " + s3Prefix);
            String accountId = getCallerAccountId();
            logger.debug(" caller accountID : " + accountId);
            logger.debug(" permission : " + permission);

            AWSCredentials credentials = getCredentialsFromAccessGrants(awsS3ControlClient, permission, s3Prefix, accountId);

            return new AWSStaticCredentialsProvider(credentials);
        } catch (AmazonServiceException e) {
            if (shouldFallbackToDefaultCredentialsForThisCase(e.getCause())) {
                return credentialsProvider;
            }
            throw e;
        }
    }

    /**
     * this method decides which credentials to return in case there is a problem fetching credentials from Access Grants
     * @param cause Cause of the exception
     * @return if to return original credentials set by customer
     */
    @VisibleForTesting
    boolean shouldFallbackToDefaultCredentialsForThisCase(Throwable cause) {
        if(enableFallback) {
            logger.debug(" Fall back enabled on the plugin! falling back to evaluate permission through policies!");
            return true;
        }
        if(cause instanceof UnsupportedOperationException) {
            logger.debug(" Operation not supported by S3 access grants! fall back to evaluate permission through policies!");
            return true;
        }
        return false;
    }

    /**
     * calls STS to get the caller identity
     * @return accountId of the caller
     */
    @VisibleForTesting
    String getCallerAccountId() {
        String accountId = stsClient.getCallerIdentity(new GetCallerIdentityRequest()).getAccount();
        S3AccessGrantsUtils.argumentNotNull(accountId, "An internal exception has occurred. Expecting account Id to be specified for the request.");
        return accountId;
    }

    /**
     * *
     * @param permission Permission required to perform an operation
     * @param s3Prefix s3Prefix of the bucket to get the credentials for
     * @param accountId Account Id of the requester
     * @return Credentials from Access Grants
     */
    @VisibleForTesting
    AWSCredentials getCredentialsFromAccessGrants(AWSS3Control awsS3ControlClient, Permission permission, String s3Prefix, String accountId) {
        return cacheImpl.getDataAccess(awsS3ControlClient, credentialsProvider.getCredentials(), permission, s3Prefix, accountId);
    }

    /**
     * *
     * @param s3Client used to make headBucket() call
     * @param s3Prefix 3Prefix of the bucket to get the credentials for
     * @return S3ControlClient for the region the bucket is in
     */
    @VisibleForTesting
    AWSS3Control getS3ControlClientForRegion(AmazonS3 s3Client, String s3Prefix) {
        String bucketName = s3Prefix.split("/")[2];
        Regions region = cacheImpl.getBucketRegion(s3Client, bucketName);
        AWSS3Control s3ControlClient = clientsCache.get(region);

        if (s3ControlClient == null) {
            s3ControlClient = AWSS3ControlClientBuilder.standard()
                    .withRegion(region)
                    .withCredentials(credentialsProvider)
                    .build();
            clientsCache.put(region, s3ControlClient);
        }
        return s3ControlClient;
    }

    /**
     * *
     * @return map of S3ControlClients
     */
    @VisibleForTesting
    ConcurrentHashMap<Regions, AWSS3Control> getClientsCache(){
        return this.clientsCache;
    }

}
