package local.assignment;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.github.cdimascio.dotenv.Dotenv;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.IamInstanceProfileSpecification;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.StartInstancesRequest;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class AWS {

    public Region region1 = Region.US_WEST_2;
    protected final S3Client s3;
    protected final SqsClient sqs;
    protected final Ec2Client ec2;
    protected final String bucketName;
    protected static AWS instance = null;
    protected final String ami = "ami-0412b8b02f19cbcf0";

    //Loading necessary keys for AWS CLI
    protected Dotenv dotenv = Dotenv.load();
    protected String accessKey = dotenv.get("AWS_ACCESS_KEY_ID");
    protected String secretKey = dotenv.get("AWS_SECRET_ACCESS_KEY");
    protected String sessionToken = dotenv.get("AWS_SESSION_TOKEN");

    protected AWS() {
        this.s3 = S3Client.builder().region(region1).build();
        this.sqs = SqsClient.builder().region(region1).build();
        this.ec2 = Ec2Client.builder().region(region1).build();
        this.bucketName = "beni-haagadi";
    }

    public static AWS getInstance() {
        if (instance == null) {
            instance = new AWS();
        }
        return instance;
    }

    ////////////////////////////////////////// EC2

    public void startInstance(String instanceId) {
        try {
            StartInstancesRequest startRequest = StartInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();
            ec2.startInstances(startRequest);
            System.out.println("Manager node activated: " + instanceId);
        } catch (Exception e) {
            System.err.println("Failed to start instance: " + instanceId);
            e.printStackTrace();
        }
    }

    public void runInstanceFromAMI(String ami) {
        RunInstancesRequest runInstancesRequest = RunInstancesRequest.builder()
                .imageId(ami)
                .instanceType(InstanceType.T2_NANO)
                .minCount(1)
                .maxCount(5) // Adjust as necessary
                .build();

        try {
            ec2.runInstances(runInstancesRequest);
        } catch (Exception e) {
            throw new RuntimeException("Failed to launch instance", e);
        }
    }

    public RunInstancesResponse runInstanceFromAmiWithScript(InstanceType instanceType, int min, int max,
            String tagValue) {
        String script = getUserDataScript(tagValue);
        String ec2Role = "LabInstanceProfile";
        RunInstancesRequest runInstancesRequest = RunInstancesRequest.builder()
                .imageId(ami)
                .instanceType(instanceType)
                .minCount(min)
                .maxCount(max)
                .userData(Base64.getEncoder().encodeToString(script.getBytes(StandardCharsets.UTF_8)))
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name(ec2Role).build())
                .build();

        try {
            return ec2.runInstances(runInstancesRequest);
        } catch (Exception e) {
            throw new RuntimeException("Failed to launch instance with script", e);
        }
    }

    protected String getUserDataScript(String tagValue) {
        // Customize this script to download and run the correct JAR file based on the instance tag
        String s3Key = tagValue.equals("Manager") ? "manager.jar" : "worker.jar";
    
        // Return the updated user data script with credentials, region, and aws configure commands
        return "#!/bin/bash\n" +
                "cd /home/ec2-user\n" +
                // Set AWS credentials using aws configure
                "aws configure set aws_access_key_id " + accessKey + "\n" +
                "aws configure set aws_secret_access_key " + secretKey + "\n" +
                "aws configure set aws_session_token " + sessionToken + "\n" +
                "aws configure set region " + region1 + "\n" +
                
                // Download the correct JAR file from S3
                "aws s3 cp s3://" + bucketName + "/jars/" + s3Key + " /home/ec2-user/" + s3Key + "\n" +
                
                // Run the JAR file
                "java -jar /home/ec2-user/" + s3Key + "\n";
    }
    
    

    public List<Instance> getAllInstances() {
        DescribeInstancesRequest describeInstancesRequest = DescribeInstancesRequest.builder().build();
        DescribeInstancesResponse response = ec2.describeInstances(describeInstancesRequest);

        return response.reservations().stream()
                .flatMap(r -> r.instances().stream())
                .toList();
    }

    public List<Instance> getAllInstancesWithLabel(Label label) {
        DescribeInstancesRequest describeInstancesRequest = DescribeInstancesRequest.builder()
                .filters(Filter.builder()
                        .name("tag:Label")
                        .values(label.name())
                        .build())
                .build();

        DescribeInstancesResponse response = ec2.describeInstances(describeInstancesRequest);
        return response.reservations().stream()
                .flatMap(r -> r.instances().stream())
                .toList();
    }

    public void terminateInstance(String instanceId) {
        TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        try {
            ec2.terminateInstances(terminateRequest);
        } catch (Exception e) {
            throw new RuntimeException("Failed to terminate instance: " + instanceId, e);
        }

        System.out.println("Terminated instance: " + instanceId);
    }

    ////////////////////////////// S3

    public String uploadFileToS3(String keyPath, File file) throws Exception {
        System.out.printf("Start upload: %s, to S3\n", file.getName());

        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(keyPath)
                .build();

        s3.putObject(request, file.toPath());
        return "s3://" + bucketName + "/" + keyPath;
    }

    public void downloadFileFromS3(String keyPath, File outputFile) {
        System.out.println("Start downloading file " + keyPath + " to " + outputFile.getPath());

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(keyPath)
                .build();

        ResponseBytes<GetObjectResponse> objectBytes = null;
        OutputStream os = null;

        try {
            // Retrieve the object bytes from S3
            objectBytes = s3.getObjectAsBytes(getObjectRequest);

            // Prepare the output stream for saving the file
            os = new FileOutputStream(outputFile);

            // Convert the object bytes to a byte array and write to the output file
            byte[] data = objectBytes.asByteArray();
            os.write(data);
            System.out.println("Successfully obtained bytes from an S3 object");

        } catch (Exception e) {
            throw new RuntimeException("Failed to download file from S3", e);
        } finally {
            // Close the resources manually
            if (objectBytes != null) {
                // No need to close objectBytes, since it does not implement AutoCloseable.
                // However, you need to close the InputStream that may be used internally.
                try (InputStream inputStream = objectBytes.asInputStream()) {
                    if (inputStream != null) {
                        inputStream.close();
                    }
                } catch (IOException ex) {
                    // Handle error closing the InputStream
                    ex.printStackTrace();
                }
            }

            // Close the OutputStream
            if (os != null) {
                try {
                    os.close();
                } catch (IOException ex) {
                    // Handle error closing the OutputStream
                    ex.printStackTrace();
                }
            }
        }
    }

    public void createBucket(String bucketName) {
        s3.createBucket(CreateBucketRequest.builder()
                .bucket(bucketName)
                .build());
        s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                .bucket(bucketName)
                .build());
    }

    public void deleteBucket(String bucketName) {
        deleteAllObjectsFromBucket(bucketName);
        deleteEmptyBucket(bucketName);
    }

    public boolean checkIfBucketExists(String bucketName) {
        try {
            // Try to get the head information of the bucket (this will throw an exception
            // if the bucket doesn't exist)
            HeadBucketRequest request = HeadBucketRequest.builder().bucket(bucketName).build();
            s3.headBucket(request);
            return true; // If no exception, the bucket exists
        } catch (NoSuchBucketException e) {
            // If NoSuchBucketException is caught, the bucket doesn't exist
            System.out.println("Bucket " + bucketName + " does not exist.");
            return false;
        } catch (BucketAlreadyOwnedByYouException e) {
            // If the bucket already exists and is owned by you, handle that case
            System.out.println("Bucket " + bucketName + " is already owned by you.");
            return true; // The bucket exists and is owned by you
        } catch (Exception e) {
            // Handle other exceptions (e.g., permission issues)
            System.err.println("Error checking if bucket exists: " + e.getMessage());
            return false;
        }
    }

    public void deleteAllObjectsFromBucket(String bucketName) {
        // List objects from the bucket (Paginator or Iterable)
        ListObjectsV2Iterable contents = listObjectsInBucket(bucketName);

        // Initialize the list for ObjectIdentifiers
        List<ObjectIdentifier> keys = new ArrayList<>();

        // Iterate through each page of objects in the paginator
        for (ListObjectsV2Response page : contents) {
            // Add each object key to the list
            for (S3Object content : page.contents()) {
                keys.add(ObjectIdentifier.builder().key(content.key()).build());
            }
        }

        // Prepare the Delete request
        Delete del = Delete.builder().objects(keys).build();
        DeleteObjectsRequest multiObjectDeleteRequest = DeleteObjectsRequest.builder()
                .bucket(bucketName)
                .delete(del)
                .build();

        try {
            // Execute the delete operation
            s3.deleteObjects(multiObjectDeleteRequest);
            System.out.println("Successfully deleted all objects from the bucket");
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete objects from bucket", e);
        }
    }

    private void deleteEmptyBucket(String bucketName) {
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder()
                .bucket(bucketName)
                .build();
        try {
            s3.deleteBucket(deleteBucketRequest);
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete bucket", e);
        }
    }

    public ListObjectsV2Iterable listObjectsInBucket(String bucketName) {
        ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .maxKeys(1)
                .build();

        try {
            return s3.listObjectsV2Paginator(listReq);
        } catch (Exception e) {
            throw new RuntimeException("Failed to list objects in bucket", e);
        }
    }

    public void tagInstanceAsManager(String instanceId) {
        try {
            Tag managerTag = Tag.builder().key("Label").value(Label.Manager.name()).build();
            CreateTagsRequest createTagsRequest = CreateTagsRequest.builder()
                    .resources(instanceId)
                    .tags(managerTag)
                    .build();
            ec2.createTags(createTagsRequest);
            System.out.println("New Manager node tagged: " + instanceId);
        } catch (Exception e) {
            System.err.println("Failed to tag instance: " + instanceId);
            e.printStackTrace();
        }
    }

    public void tagInstanceAsWorker(String instanceId) {
        try {
            Tag workerTag = Tag.builder().key("Label").value(Label.Worker.name()).build();
            CreateTagsRequest createTagsRequest = CreateTagsRequest.builder()
                    .resources(instanceId)
                    .tags(workerTag)
                    .build();
            ec2.createTags(createTagsRequest);
            System.out.println("New Worker node tagged: " + instanceId);
        } catch (Exception e) {
            System.err.println("Failed to tag instance: " + instanceId);
            e.printStackTrace();
        }
    }

    public List<String> getFilesInFolder(String folderPath) {
        List<String> fileKeys = new ArrayList<>();

        try {
            ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(folderPath) // Ensure you fetch only items under the folder
                    .build();

            ListObjectsV2Response listRes;
            do {
                listRes = s3.listObjectsV2(listReq);
                for (S3Object s3Object : listRes.contents())
                    fileKeys.add(s3Object.key());

                // Prepare the next request in case of pagination
                listReq = listReq.toBuilder()
                        .continuationToken(listRes.nextContinuationToken())
                        .build();

            } while (listRes.isTruncated());
        } catch (Exception e) {
            throw new RuntimeException("Failed to list files in folder: " + folderPath, e);
        }
        return fileKeys;
    }

    public void deleteAllFilesInFolder(String folderPrefix) {
        // Ensure folderPrefix ends with "/"
        if (!folderPrefix.endsWith("/")) {
            folderPrefix += "/";
        }

        // List all objects in the specified folder
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(folderPrefix)
                .build();

        ListObjectsV2Response listResponse = s3.listObjectsV2(listRequest);

        // Iterate through each object and delete it
        for (S3Object s3Object : listResponse.contents()) {
            String objectKey = s3Object.key();
            DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .build();

            s3.deleteObject(deleteRequest);
            System.out.println("Deleted: " + objectKey);
        }
    }

    ////////////////////////////////////////////// SQS

    public String createQueue(String queueName) {
        CreateQueueRequest request = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();

        CreateQueueResponse create_result = sqs.createQueue(request);
        String queueUrl = create_result.queueUrl();
        System.out.println("Created queue '" + queueName + "', queue URL: " + queueUrl);
        return queueUrl;
    }

    public void deleteQueue(String queueUrl) {
        DeleteQueueRequest req = DeleteQueueRequest.builder()
                .queueUrl(queueUrl)
                .build();
        sqs.deleteQueue(req);
    }

    public String getQueueUrl(String queueName) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();

        return sqs.getQueueUrl(getQueueRequest).queueUrl();
    }

    public int getQueueSize(String queueUrl) {
        GetQueueAttributesRequest getQueueAttributesRequest = GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
                .build();

        GetQueueAttributesResponse queueAttributesResponse = sqs.getQueueAttributes(getQueueAttributesRequest);
        Map<QueueAttributeName, String> attributes = queueAttributesResponse.attributes();

        return Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES));
    }

    public void sendSQSMessage(String messageBody, String SQSUrl) {
        try {
            SendMessageRequest request = SendMessageRequest.builder()
                    .queueUrl(SQSUrl)
                    .messageBody(messageBody)
                    .build();

            sqs.sendMessage(request);
            System.out.println("Message sent to queue: " + SQSUrl + "\nMessage Sent: " + messageBody);
        } catch (SdkException e) { // Catching AWS SDK exceptions
            System.err.println("Failed to send message to queue: " + SQSUrl);
            System.err.println("Error: " + e.getMessage());
        }
    }

    public List<Message> getSQSMessagesList(String queueUrl, Integer maxNumberOfMessages, Integer waitTimeSeconds) {
        try {
            // Create a ReceiveMessageRequest to fetch messages from the queue
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(maxNumberOfMessages) // Fetch up to x messages at a time
                    .waitTimeSeconds(waitTimeSeconds) // Long polling for y seconds
                    .build();

            // Fetch messages
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();

            if (messages.isEmpty()) {
                System.out.println("No messages received yet.");
            }
            return messages;

        } catch (Exception e) {
            System.err.println("Error while receiving messages: " + e.getMessage());
            return Collections.emptyList(); // Return an empty list in case of an exception
        }
    }

    public void deleteMessage(String queueUrl, String receiptHandle) {
        try {
            // Create a DeleteMessageRequest
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(receiptHandle)
                    .build();

            // Delete the message from the queue
            sqs.deleteMessage(deleteMessageRequest);
            System.out.println("Message deleted successfully.");
        } catch (Exception e) {
            System.err.println("Error while deleting message: " + e.getMessage());
        }
    }

    ///////////////////////

    public enum Label {
        Manager,
        Worker
    }
}
