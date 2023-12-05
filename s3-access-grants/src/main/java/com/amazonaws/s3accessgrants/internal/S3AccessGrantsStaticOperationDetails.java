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
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectAclRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SetObjectAclRequest;
import com.amazonaws.services.s3control.model.Permission;

import java.util.HashMap;

public class S3AccessGrantsStaticOperationDetails {

    private static final HashMap<String, Permission> supportedAccessGrantsOperations =  new HashMap<>();

    static {
        supportedAccessGrantsOperations.put("GETOBJECTREQUEST", Permission.READ);
        supportedAccessGrantsOperations.put("GETOBJECTACLREQUEST", Permission.READ);
        supportedAccessGrantsOperations.put("LISTMULTIPARTUPLOADSREQUEST", Permission.READ);
        supportedAccessGrantsOperations.put("LISTOBJECTSREQUEST", Permission.READ);
        supportedAccessGrantsOperations.put("LISTOBJECTSV2REQUEST", Permission.READ);
        supportedAccessGrantsOperations.put("LISTVERSIONSREQUEST", Permission.READ);

        supportedAccessGrantsOperations.put("PUTOBJECTREQUEST", Permission.WRITE);
        supportedAccessGrantsOperations.put("SETOBJECTACLREQUEST", Permission.WRITE);
        supportedAccessGrantsOperations.put("DELETEOBJECTREQUEST", Permission.WRITE);
        supportedAccessGrantsOperations.put("ABORTMULTIPARTUPLOADREQUEST", Permission.WRITE);
        supportedAccessGrantsOperations.put("CREATEMULTIPARTUPLOAD", Permission.WRITE);
        supportedAccessGrantsOperations.put("UPLOADPART", Permission.WRITE);
        supportedAccessGrantsOperations.put("COMPLETEMULTIPARTUPLOAD", Permission.WRITE);

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
            ListObjectsRequest listObjectsRequest = (ListObjectsRequest) request;
            if (listObjectsRequest.getPrefix() == null) {
                s3Prefix = "s3://" + listObjectsRequest.getBucketName();
            } else {
                s3Prefix = "s3://" + listObjectsRequest.getBucketName() + "/" + listObjectsRequest.getPrefix();
            }
        }
        else if (request instanceof ListObjectsV2Request) {
            ListObjectsV2Request listObjectsRequest = (ListObjectsV2Request) request;
            if (listObjectsRequest.getPrefix() == null) {
                s3Prefix = "s3://" + listObjectsRequest.getBucketName();
            } else {
                s3Prefix = "s3://" + listObjectsRequest.getBucketName() + "/" + listObjectsRequest.getPrefix();
            }
        }
        else if (request instanceof PutObjectRequest) {
            PutObjectRequest putObjectRequest = (PutObjectRequest) request;
            s3Prefix = "s3://" + putObjectRequest.getBucketName() + "/" +putObjectRequest.getKey();
        }
        else if (request instanceof DeleteObjectRequest) {
            DeleteObjectRequest deleteObjectRequest = (DeleteObjectRequest) request;
            s3Prefix = "s3://" + deleteObjectRequest.getBucketName() + "/" +deleteObjectRequest.getKey();
            System.out.println(" prefix in delete object" +s3Prefix);
        }
        else if (request instanceof SetObjectAclRequest) {
            SetObjectAclRequest setObjectAclRequest = (SetObjectAclRequest) request;
            s3Prefix = "s3://" + setObjectAclRequest.getBucketName() + "/" +setObjectAclRequest.getKey();
            System.out.println(" prefix in delete object" +s3Prefix);
        }
        else if (request instanceof GetObjectAclRequest) {
            GetObjectAclRequest getObjectAclRequest = (GetObjectAclRequest) request;
            s3Prefix = "s3://" + getObjectAclRequest.getBucketName() + "/" +getObjectAclRequest.getKey();
        }
        //Todo: Add details for rest of the requests
        return s3Prefix;
    }
}
