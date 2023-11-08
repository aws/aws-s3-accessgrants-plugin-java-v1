package com.amazonaws.s3accessgrants.cache;

import com.amazonaws.services.s3control.model.AWSS3ControlException;

public interface S3AccessGrantsAccountIdResolver {
    /**
     *
     * @param accountId AWS AccountId from the request context parameter
     * @param s3Prefix e.g., s3://bucket-name/path/to/helloworld.txt
     * @return AWS AccountId of the S3 Access Grants Instance that owns the location scope of the s3Prefix
     * @throws AWSS3ControlException propagate S3ControlException from service call
     */
    String resolve(String accountId, String s3Prefix) throws AWSS3ControlException;
}
