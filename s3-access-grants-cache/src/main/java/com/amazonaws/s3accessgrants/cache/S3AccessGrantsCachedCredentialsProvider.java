package com.amazonaws.s3accessgrants.cache;

import com.amazonaws.auth.AWSCredentials;
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
    AWSCredentials getDataAccess (AWSCredentials credentials, Permission permission, String s3Prefix,
                                         String accountId) throws AWSS3ControlException;
}
