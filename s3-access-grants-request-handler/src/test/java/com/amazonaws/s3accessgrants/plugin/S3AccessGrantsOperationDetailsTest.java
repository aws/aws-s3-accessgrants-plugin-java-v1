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

package com.amazonaws.s3accessgrants.plugin;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.s3accessgrants.plugin.internal.S3AccessGrantsStaticOperationDetails;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3control.model.Permission;
import org.junit.Test;

import java.util.ArrayList;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class S3AccessGrantsOperationDetailsTest {
    private final S3AccessGrantsStaticOperationDetails operationDetails = new S3AccessGrantsStaticOperationDetails();

    @Test
    public void testGetOperation() {
        //When
        String requestClass = "com.amazonaws.service.GetObjectRequest";
        //Then
        assertThat(operationDetails.getOperation(requestClass)).isEqualTo("GetObjectRequest");
    }

    @Test(expected = java.lang.IllegalArgumentException.class)
    public void testGetOperationThrowException() {
        //When
        String requestClass = null;
        //Then
        operationDetails.getOperation(requestClass);
    }

    @Test
    public void testGetPermission() {
        //When
        String operation = "GetObjectRequest";
        //Then
        assertThat(operationDetails.getPermission(operation)).isEqualTo(Permission.READ);
    }

    @Test
    public void testGetPermissionThrowException() {
        //When
        String operation = "UnsupportedOperation";
        try{
            operationDetails.getPermission(operation);
        } catch (AmazonServiceException e){
            //Then
            assertThat(e.getCause().getMessage()).isEqualTo("Access Grants does not support the requested operation!");
        }
    }

    @Test
    public void testGetPath() {
        //When
        AmazonWebServiceRequest getObjectRequest = new GetObjectRequest("test-bucket", "PrefixA/file1.txt");
        //Then
        assertThat(operationDetails.getPath(getObjectRequest)).isEqualTo("s3://test-bucket/PrefixA/file1.txt");
    }

    @Test(expected = java.lang.IllegalArgumentException.class)
    public void testGetPathThrowException() {
        //When
        AmazonWebServiceRequest request = null;
        //Then
        operationDetails.getPath(request);
    }

    @Test
    public void getCommonPrefixFromMultiplePrefixes() {
        ArrayList<String> keys1 = new ArrayList<>();
        keys1.add("A/B/C/log.txt");
        keys1.add("B/A/C/log.txt");
        keys1.add("C/A/B/log.txt");
        assertThat(operationDetails.getCommonPrefixFromMultiplePrefixes(keys1)).isEqualTo("/*");
        ArrayList<String> keys2 = new ArrayList<>();
        keys2.add("ABC/A/B/C/log.txt");
        keys2.add("ABC/B/A/C/log.txt");
        keys2.add("ABC/C/A/B/log.txt");
        assertThat(operationDetails.getCommonPrefixFromMultiplePrefixes(keys2)).isEqualTo("/ABC/*");
        ArrayList<String> keys3 = new ArrayList<>();
        keys3.add("ABC/A/B/C/log.txt");
        keys3.add("ABC/B/A/C/log.txt");
        keys3.add("ABC/C/A/B/log.txt");
        keys3.add("XYZ/X/Y/Y/log.txt");
        keys3.add("XYZ/Y/X/Z/log.txt");
        keys3.add("XYZ/Z/X/Y/log.txt");
        assertThat(operationDetails.getCommonPrefixFromMultiplePrefixes(keys3)).isEqualTo("/*");
    }
}
