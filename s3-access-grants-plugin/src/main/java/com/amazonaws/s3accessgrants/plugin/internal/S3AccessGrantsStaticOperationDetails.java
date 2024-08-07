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

package com.amazonaws.s3accessgrants.plugin.internal;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectAclRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SetObjectAclRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3control.model.Permission;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class S3AccessGrantsStaticOperationDetails {

    private static final HashMap<String, Permission> supportedAccessGrantsOperations =  new HashMap<>();
    private static final Log logger = LogFactory.getLog(S3AccessGrantsStaticOperationDetails.class);

    static {
        supportedAccessGrantsOperations.put("GETOBJECTREQUEST", Permission.READ);
        supportedAccessGrantsOperations.put("GETOBJECTACLREQUEST", Permission.READ);
        supportedAccessGrantsOperations.put("LISTMULTIPARTUPLOADSREQUEST", Permission.READ); 
        supportedAccessGrantsOperations.put("LISTOBJECTSREQUEST", Permission.READ); 
        supportedAccessGrantsOperations.put("LISTOBJECTSV2REQUEST", Permission.READ);
        supportedAccessGrantsOperations.put("LISTVERSIONSREQUEST", Permission.READ);
        supportedAccessGrantsOperations.put("GETOBJECTMETADATAREQUEST", Permission.READ);
        supportedAccessGrantsOperations.put("HEADBUCKETREQUEST", Permission.READ);
        supportedAccessGrantsOperations.put("LISTPARTSREQUEST", Permission.READ);
        supportedAccessGrantsOperations.put("PUTOBJECTREQUEST", Permission.WRITE); 
        supportedAccessGrantsOperations.put("SETOBJECTACLREQUEST", Permission.WRITE); 
        supportedAccessGrantsOperations.put("DELETEOBJECTREQUEST", Permission.WRITE); 
        supportedAccessGrantsOperations.put("ABORTMULTIPARTUPLOADREQUEST", Permission.WRITE); 
        supportedAccessGrantsOperations.put("INITIATEMULTIPARTUPLOADREQUEST", Permission.WRITE);
        supportedAccessGrantsOperations.put("UPLOADPARTREQUEST", Permission.WRITE);
        supportedAccessGrantsOperations.put("COMPLETEMULTIPARTUPLOADREQUEST", Permission.WRITE);
        supportedAccessGrantsOperations.put("DELETEOBJECTSREQUEST", Permission.WRITE);
        supportedAccessGrantsOperations.put("COPYOBJECTREQUEST", Permission.READWRITE);
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
        }
        else if (request instanceof SetObjectAclRequest) {
            SetObjectAclRequest setObjectAclRequest = (SetObjectAclRequest) request;
            s3Prefix = "s3://" + setObjectAclRequest.getBucketName() + "/" +setObjectAclRequest.getKey();
        }
        else if (request instanceof GetObjectAclRequest) {
            GetObjectAclRequest getObjectAclRequest = (GetObjectAclRequest) request;
            s3Prefix = "s3://" + getObjectAclRequest.getBucketName() + "/" +getObjectAclRequest.getKey();
        }
        else if (request instanceof ListVersionsRequest) {
            ListVersionsRequest listVersionsRequest = (ListVersionsRequest) request;
            if (listVersionsRequest.getPrefix() == null){
                s3Prefix = "s3://" + listVersionsRequest.getBucketName();
            } else {
                s3Prefix = "s3://" + listVersionsRequest.getBucketName() + "/" + listVersionsRequest.getPrefix();
            }
        }
        else if (request instanceof ListMultipartUploadsRequest) {
            ListMultipartUploadsRequest listMultipartUploadsRequest = (ListMultipartUploadsRequest) request;
            if (listMultipartUploadsRequest.getPrefix() == null){
                s3Prefix = "s3://" + listMultipartUploadsRequest.getBucketName();
            } else {
                s3Prefix = "s3://" + listMultipartUploadsRequest.getBucketName() + "/" + listMultipartUploadsRequest.getPrefix();
            }
        }
        else if (request instanceof InitiateMultipartUploadRequest) {
            InitiateMultipartUploadRequest initiateMultipartUploadRequest = (InitiateMultipartUploadRequest) request;
            if (initiateMultipartUploadRequest.getKey() == null){
                s3Prefix = "s3://" + initiateMultipartUploadRequest.getBucketName();
            } else {
                s3Prefix = "s3://" + initiateMultipartUploadRequest.getBucketName() + "/" + initiateMultipartUploadRequest.getKey();
            }
        }
        else if (request instanceof UploadPartRequest) {
            UploadPartRequest uploadPartRequest = (UploadPartRequest) request;
            if (uploadPartRequest.getKey() == null){
                s3Prefix = "s3://" + uploadPartRequest.getBucketName();
            } else {
                s3Prefix = "s3://" + uploadPartRequest.getBucketName() + "/" + uploadPartRequest.getKey();
            }
        }
        else if (request instanceof CompleteMultipartUploadRequest) {
            CompleteMultipartUploadRequest completeMultipartUploadRequest = (CompleteMultipartUploadRequest) request;
            if (completeMultipartUploadRequest.getKey() == null){
                s3Prefix = "s3://" + completeMultipartUploadRequest.getBucketName();
            } else {
                s3Prefix = "s3://" + completeMultipartUploadRequest.getBucketName() + "/" + completeMultipartUploadRequest.getKey();
            }
        }
        else if (request instanceof AbortMultipartUploadRequest) {
            AbortMultipartUploadRequest abortMultipartUploadRequest = (AbortMultipartUploadRequest) request;
            if (abortMultipartUploadRequest.getKey() == null){
                s3Prefix = "s3://" + abortMultipartUploadRequest.getBucketName();
            } else {
                s3Prefix = "s3://" + abortMultipartUploadRequest.getBucketName() + "/" + abortMultipartUploadRequest.getKey();
            }
        }
        else if (request instanceof GetObjectMetadataRequest) {
            GetObjectMetadataRequest getObjectMetadataRequest = (GetObjectMetadataRequest) request;
                s3Prefix = "s3://" + getObjectMetadataRequest.getBucketName() + "/" + getObjectMetadataRequest.getKey();
        }
        else if (request instanceof HeadBucketRequest) {
            HeadBucketRequest headBucketRequest = (HeadBucketRequest) request;
            s3Prefix = "s3://" + headBucketRequest.getBucketName();
        }
        else if (request instanceof DeleteObjectsRequest) {
            DeleteObjectsRequest deleteObjectRequest = (DeleteObjectsRequest) request;
            List<DeleteObjectsRequest.KeyVersion> keyList = deleteObjectRequest.getKeys();
            ArrayList<String> objectKeysToDelete = new ArrayList<>();
            for (DeleteObjectsRequest.KeyVersion i : keyList) {
                objectKeysToDelete.add(i.getKey());
            }
            s3Prefix = "s3://" + deleteObjectRequest.getBucketName() + getCommonPrefixFromMultiplePrefixes(objectKeysToDelete);
        }
        else if (request instanceof CopyObjectRequest) {
            CopyObjectRequest copyObjectRequest = (CopyObjectRequest) request;
            if (!copyObjectRequest.getSourceBucketName().equals(copyObjectRequest.getDestinationBucketName())){
                logger.debug("Source and destination buckets are different for copy request. Access Grants does not support this use-case.");
                throw new AmazonServiceException("The requested operation cannot be completed!", new UnsupportedOperationException("Access Grants does not support the requested operation!"));
            } else {
                ArrayList<String> keysList = new ArrayList<>();
                keysList.add(copyObjectRequest.getSourceKey());
                keysList.add(copyObjectRequest.getDestinationKey());
                s3Prefix = "s3://" + copyObjectRequest.getSourceBucketName() + getCommonPrefixFromMultiplePrefixes(keysList);
            }
        }
        else if (request instanceof ListPartsRequest) {
            ListPartsRequest listPartsRequest = (ListPartsRequest) request;
            if (listPartsRequest.getKey() == null){
                s3Prefix = "s3://" + listPartsRequest.getBucketName();
            } else {
                s3Prefix = "s3://" + listPartsRequest.getBucketName() + "/" + listPartsRequest.getKey();
            }
        }
        return s3Prefix;
    }

    public String getCommonPrefixFromMultiplePrefixes(ArrayList<String> keys) {
        String commonAncestor = keys.get(0);
        String lastPrefix = "";
        for (String i : keys) {
            while(!commonAncestor.equals("")) {
                if (!i.startsWith(commonAncestor)){
                    int lastIndex = commonAncestor.lastIndexOf("/");
                    if (lastIndex == -1){
                        return "/";
                    }
                    lastPrefix = commonAncestor.substring(lastIndex+1);
                    commonAncestor = commonAncestor.substring(0, lastIndex);
                } else {
                    break;
                }
            }
        }

        String newCommonAncestor = commonAncestor + "/" + lastPrefix;
        for (String i : keys) {
            while(!lastPrefix.equals("")){
                if (!i.startsWith(newCommonAncestor)){
                    lastPrefix = lastPrefix.substring(0, lastPrefix.length()-1);
                    newCommonAncestor = commonAncestor + "/" + lastPrefix;
                }
                else{
                    break;
                }
            }
        }
        return "/" + newCommonAncestor;
    }
}
