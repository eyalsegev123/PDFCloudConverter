package local.assignment;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;


import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class AWS {

    public final String IMAGE_AMI = "ami-04aabd45b36980079";
    public Region region1 = Region.US_WEST_2;
    protected final S3Client s3;
    protected final SqsClient sqs;
    protected final Ec2Client ec2;
    protected final String bucketName;
    protected static AWS instance = null;
    
    protected AWS() {
        this.s3 = S3Client.builder().region(region1).build();
        this.sqs = SqsClient.builder().region(region1).build();
        this.ec2 = Ec2Client.builder().region(region1).build();
        this.bucketName = "malawach";
    }

    public static AWS getInstance() {
        if (instance == null) {
            instance = new AWS();
        }
        return instance;
    }

    ////////////////////////////////////////// EC2

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

    public RunInstancesResponse runInstanceFromAmiWithScript(String ami, InstanceType instanceType, int min, int max, String script) {
        RunInstancesRequest runInstancesRequest = RunInstancesRequest.builder()
                .imageId(ami)
                .instanceType(instanceType)
                .minCount(min)
                .maxCount(max)
                .userData(Base64.getEncoder().encodeToString(script.getBytes(StandardCharsets.UTF_8)))
                .build();

        try {
            return ec2.runInstances(runInstancesRequest);
        } catch (Exception e) {
            throw new RuntimeException("Failed to launch instance with script", e);
        }
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
                        .values(label.toString())
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
    
        } 
        catch (Exception e) {
            throw new RuntimeException("Failed to download file from S3", e);
        } 
        finally {
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
            // Try to get the head information of the bucket (this will throw an exception if the bucket doesn't exist)
            HeadBucketRequest request = HeadBucketRequest.builder().bucket(bucketName).build();
            s3.headBucket(request);
            return true; // If no exception, the bucket exists
        } catch (NoSuchBucketException e) {
            // If NoSuchBucketException is caught, the bucket doesn't exist
            System.out.println("Bucket " + bucketName + " does not exist.");
            return false;
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
        SendMessageRequest request = SendMessageRequest.builder()
            .queueUrl(SQSUrl)
            .messageBody(messageBody)
            .build();

        sqs.sendMessage(request);
        System.out.println("Message sent to queue: " + SQSUrl + "/nMessage Sent: " + messageBody);
    }

    public List<Message> getSQSMessagesList(String queueUrl) {
        try {
            // Create a ReceiveMessageRequest to fetch messages from the queue
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(10) // Fetch up to 10 messages at a time
                    .waitTimeSeconds(20)    // Long polling for 20 seconds
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
