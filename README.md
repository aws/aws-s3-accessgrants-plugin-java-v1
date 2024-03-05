## AWS S3 ACCESS GRANTS PLUGIN FOR AWS JAVA SDK 2.0

AWS S3 ACCESS GRANTS PLUGIN provides the functionality to enable S3 customers to configure S3 ACCESS GRANTS as a permission layer on top of the S3 Clients.

S3 ACCESS GRANTS is a feature from S3 that allows its customers to configure fine-grained access permissions for the data in their buckets.

### Things to Know

---

* AWS SDK Java 1.0 is built on Java 8
* [maven] is used as the build and dependency management system

### Contributions

---
* Use [GitHub flow](https://docs.github.com/en/get-started/quickstart/github-flow) to commit/review/collaborate on changes
* After a PR is approved/merged, please delete the PR branch both remotely and locally

### Building From Source

---
Once you check out the code from GitHub, you can build it using the following commands.

Linux:

```./mvnw clean install```

Windows:

```./mvnw.cmd clean install```
### USING THE PLUGIN

---

The recommended way to use the S3 ACCESS GRANTS PLUGIN for Java in your project is to consume it from Maven Central


```
 <dependency>
    <groupId>software.amazon.s3.accessgrants</groupId>
    <artifactId>java-sdk-v1-s3-access-grants-plugin</artifactId>
    <version>replace with latest version</version>
</dependency>
```

Create a S3AccessGrantsRequestHandler object and set the following fields:
```
S3AccessGrantsRequestHandler requestHandler = S3AccessGrantsRequestHandler.builder().enableFallback(fallback)
                .region(Regions.US_WEST_2).credentialsProvider(credentialsProvider).build();
```

Build S3Client as follows: 
````
AmazonS3 s3Client = AmazonS3Client.builder().withRequestHandlers(new RequestHandler2() {
                        @Override
                        public AmazonWebServiceRequest beforeExecution(AmazonWebServiceRequest request) {
    
                            AWSCredentialsProvider accessGrantsCredentials = requestHandler.resolve(request);
                            request.setRequestCredentialsProvider(accessGrantsCredentials);
                            return super.beforeExecution(request);
                        }
                    }).withRegion(region)
                    .withCredentials(credentialsProvider)
                    .build();
````

Choose if you want to enable fallback.
1. If enableFallback option is set to false we will fallback only in case the operation/API is not supported by Access Grants.
2. If enableFallback is set to true then we will fall back every time we are not able to get the credentials from Access Grants, no matter the reason.

While building S3AccessGrantsRequestHandler object you have to provide a credentialsProvider object which contains credentials that have access to get credentials from Access Grants. In case we fallback, these credentials will be used to make the API call.
Note - We only support IAM credentials with this release.

You also have to set a region while building the S3AccessGrantsRequestHandler object. This is the region the bucket is in. 
In case you want to make a cross region request, you have to enable it as below. You also have to set withForceGlobalBucketAccessEnabled as true in your S3Client.

```
S3AccessGrantsRequestHandler requestHandler = S3AccessGrantsRequestHandler.builder().enableFallback(fallback)
                .enableCrossRegionAccess(true)
                .region(Regions.US_WEST_2).credentialsProvider(credentialsProvider).build();
```

````
AmazonS3 s3Client = AmazonS3Client.builder().withRequestHandlers(new RequestHandler2() {
                        @Override
                        public AmazonWebServiceRequest beforeExecution(AmazonWebServiceRequest request) {
    
                            AWSCredentialsProvider accessGrantsCredentials = requestHandler.resolve(request);
                            request.setRequestCredentialsProvider(accessGrantsCredentials);
                            return super.beforeExecution(request);
                        }
                    }).withRegion(region)
                    .withCredentials(credentialsProvider)
                    .withForceGlobalBucketAccessEnabled(true)
                    .build();
````
Note - If cross-region access setting is turned on for either the S3 Client or the plugin (but not both), you might experience bucket region mismatch errors.

Using this S3Client to make API calls, you should be able to use Access Grants to get access to your resources.

### Change logging level

Turning on the AWS SDK level logging should turn on the logging for the S3 Access grants plugin. You can also control the logging for the plugin specifically by adding the below config to your log4j.properties file.

```
logger.s3accessgrants.name = software.amazon.awssdk.s3accessgrants
logger.s3accessgrants.level = debug
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
