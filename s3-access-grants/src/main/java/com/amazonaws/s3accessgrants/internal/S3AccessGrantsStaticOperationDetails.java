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

package com.amazonaws.s3accessgrants.internal;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3control.model.Permission;

import java.util.HashMap;

public class S3AccessGrantsStaticOperationDetails {

    private static final HashMap<String, Permission> supportedAccessGrantsOperations =  new HashMap<>();

    static {
        supportedAccessGrantsOperations.put("GETOBJECTREQUEST", Permission.READ);
        supportedAccessGrantsOperations.put("GETOBJECTACLREQUEST", Permission.READ);
        supportedAccessGrantsOperations.put("LISTMULTIPARTUPLOADSREQUEST", Permission.READ);
        supportedAccessGrantsOperations.put("LISTOBJECTSREQUEST", Permission.READ);
        supportedAccessGrantsOperations.put("LISTOBJECTVERSIONSREQUEST", Permission.READ);

        supportedAccessGrantsOperations.put("PUTOBJECTREQUEST", Permission.WRITE);
        supportedAccessGrantsOperations.put("PUTOBJECTACLREQUEST", Permission.WRITE);
        supportedAccessGrantsOperations.put("DELETEOBJECTREQUEST", Permission.WRITE);
        supportedAccessGrantsOperations.put("ABORTMULTIPARTUPLOADREQUEST", Permission.WRITE);

        supportedAccessGrantsOperations.put("DECRYPTREQUEST", Permission.READ);
        supportedAccessGrantsOperations.put("GENERATEDATAKEYREQUEST", Permission.WRITE);

    }

    public Permission getPermission(String operation) throws AmazonServiceException {
        S3AccessGrantsUtils.argumentNotNull(operation, "An internal exception has occurred. Expecting operation to be specified for the request.");
        if (supportedAccessGrantsOperations.containsKey(operation.toUpperCase())) {
            return supportedAccessGrantsOperations.get(operation.toUpperCase());
        }
        throw new AmazonServiceException("The requested operation cannot be completed!", new UnsupportedOperationException("Access Grants does not support the requested operation!"));
    }

    public String getOperation(String requestClass) {
        S3AccessGrantsUtils.argumentNotNull(requestClass, "An internal exception has occurred. Expecting request class to be specified.");
        return requestClass.substring(requestClass.lastIndexOf(".")+1);
    }

    public String getPath (AmazonWebServiceRequest request) {
        S3AccessGrantsUtils.argumentNotNull(request, "An internal exception has occurred. Expecting request to be specified.");
        String s3Prefix = null;
        if (request instanceof GetObjectRequest) {
            GetObjectRequest getObjectRequest = (GetObjectRequest) request;
            s3Prefix = "s3://" + getObjectRequest.getBucketName() + "/" + getObjectRequest.getKey();
        }
        else if (request instanceof ListObjectsRequest) {
            ListObjectsRequest getObjectRequest = (ListObjectsRequest) request;
            if (getObjectRequest.getPrefix() == null) {
                s3Prefix = "s3://" + getObjectRequest.getBucketName();
            } else {
                s3Prefix = "s3://" + getObjectRequest.getBucketName() + "/" + getObjectRequest.getPrefix();
            }
        }
        //Todo: Add details for rest of the requests
        return s3Prefix;
    }
}
