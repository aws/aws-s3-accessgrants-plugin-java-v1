package com.amazonaws.s3accessgrants;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.s3accessgrants.cache.S3AccessGrantsCachedCredentialsProviderImpl;
import com.amazonaws.s3accessgrants.internal.S3AccessGrantsStaticOperationDetails;
import com.amazonaws.services.s3control.AWSS3Control;
import com.amazonaws.services.s3control.AWSS3ControlClientBuilder;
import com.amazonaws.services.s3control.model.Permission;
import com.amazonaws.services.s3control.model.Privilege;
import com.amazonaws.s3accessgrants.internal.S3AccessGrantsUtils;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityResult;
import com.amazonaws.thirdparty.apache.logging.Log;
import com.amazonaws.thirdparty.apache.logging.LogFactory;
import org.assertj.core.util.VisibleForTesting;

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

    private S3AccessGrantsRequestHandler(boolean enableFallback, Privilege privilege, int duration, AWSCredentialsProvider credentialsProvider, Regions region) {
        this.enableFallback = enableFallback;
        this.privilege = privilege;
        this.duration = duration;
        this.credentialsProvider = credentialsProvider;
        this.region = region;
        this.stsClient = AWSSecurityTokenServiceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(Regions.US_EAST_2).build();
        this.awsS3ControlClient = AWSS3ControlClientBuilder.standard()
                .withRegion(region)
                .withCredentials(credentialsProvider)
                .build();
        this.cacheImpl = S3AccessGrantsCachedCredentialsProviderImpl.builder()
                .s3ControlClient(awsS3ControlClient).build();
    }

    @VisibleForTesting
    S3AccessGrantsRequestHandler(boolean enableFallback, AWSCredentialsProvider credentialsProvider, Regions region, AWSSecurityTokenService stsClient, S3AccessGrantsCachedCredentialsProviderImpl cacheImpl, S3AccessGrantsStaticOperationDetails operationDetails) {
        this.enableFallback = enableFallback;
        this.privilege = Privilege.Default;
        this.duration = 3600;
        this.credentialsProvider = credentialsProvider;
        this.region = region;
        this.stsClient = stsClient;
        this.awsS3ControlClient = null;
        this.cacheImpl = cacheImpl;
        this.operationDetails = operationDetails;
    }

    public static S3AccessGrantsRequestHandler.Builder builder() {
        return new S3AccessGrantsRequestHandler.BuilderImpl();
    }

    public interface Builder {
        S3AccessGrantsRequestHandler build();
        S3AccessGrantsRequestHandler.Builder enableFallback(boolean enableFallback);
        S3AccessGrantsRequestHandler.Builder privilege(Privilege privilege);
        S3AccessGrantsRequestHandler.Builder duration(int duration);
        S3AccessGrantsRequestHandler.Builder credentialsProvider(AWSCredentialsProvider credentialsProvider);
        S3AccessGrantsRequestHandler.Builder region(Regions region);
    }

    static final class BuilderImpl implements S3AccessGrantsRequestHandler.Builder {
        private boolean enableFallback = S3AccessGrantsUtils.DEFAULT_CACHE_SETTING;
        private Privilege privilege = Privilege.Default;
        private int duration = S3AccessGrantsUtils.DEFAULT_DURATION;
        private AWSCredentialsProvider credentialsProvider;
        private Regions region;

        @Override
        public S3AccessGrantsRequestHandler build() {
            return new S3AccessGrantsRequestHandler(enableFallback,privilege,duration, credentialsProvider, region);
        }

        @Override
        public Builder enableFallback(boolean enableFallback) {
            this.enableFallback = enableFallback;
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

        try {
            logger.debug(" Calling S3 Access Grants with the following request params! ");
            String operation = operationDetails.getOperation(request.getClass().toString());
            logger.debug("operation : " + operation);
            String s3Prefix = operationDetails.getPath(request);
            logger.debug(" S3Prefix : " + s3Prefix);
            String accountId = getCallerAccountId();
            logger.debug(" caller accountID : " + accountId);
            Permission permission = operationDetails.getPermission(operation);
            logger.debug(" permission : " + permission);

            AWSCredentials credentials = getCredentialsFromAccessGrants(permission, s3Prefix, accountId);

            return new AWSStaticCredentialsProvider(credentials);
        } catch (AmazonServiceException e) {
            if (shouldFallbackToDefaultCredentialsForThisCase(e.getCause())){
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
    Boolean shouldFallbackToDefaultCredentialsForThisCase(Throwable cause) {
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

    private AWSCredentials getCredentialsFromAccessGrants (Permission permission, String s3Prefix, String accountId) {
        return cacheImpl.getDataAccess(credentialsProvider.getCredentials(), permission, s3Prefix, accountId);
    }


}
