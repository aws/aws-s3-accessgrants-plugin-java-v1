package com.amazonaws.s3accessgrants;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.s3accessgrants.internal.S3AccessGrantsStaticOperationDetails;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3control.model.Permission;
import org.junit.Test;
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
}
