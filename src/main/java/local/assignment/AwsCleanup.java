package local.assignment;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

public class AwsCleanup {

    public static void main(String[] args) {
        try {
            deleteEc2Instances();
            deleteSqsQueues();
            cleanS3Buckets();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void deleteEc2Instances() {
        try (Ec2Client ec2 = Ec2Client.create()) {
            DescribeInstancesResponse response = ec2.describeInstances();
            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    String instanceId = instance.instanceId();
                    ec2.terminateInstances(TerminateInstancesRequest.builder()
                            .instanceIds(instanceId)
                            .build());
                    System.out.println("Terminated EC2 instance: " + instanceId);
                }
            }
        } catch (Ec2Exception e) {
            System.err.println("Error deleting EC2 instances: " + e.getMessage());
        }
    }

    private static void deleteSqsQueues() {
        try (SqsClient sqs = SqsClient.create()) {
            ListQueuesResponse response = sqs.listQueues();
            for (String queueUrl : response.queueUrls()) {
                sqs.deleteQueue(DeleteQueueRequest.builder()
                        .queueUrl(queueUrl)
                        .build());
                System.out.println("Deleted SQS queue: " + queueUrl);
            }
        } catch (SqsException e) {
            System.err.println("Error deleting SQS queues: " + e.getMessage());
        }
    }

    private static void cleanS3Buckets() {
        try (S3Client s3 = S3Client.create()) {
            ListBucketsResponse bucketsResponse = s3.listBuckets();
            for (Bucket bucket : bucketsResponse.buckets()) {
                String bucketName = bucket.name();
                ListObjectsV2Response objectsResponse = s3.listObjectsV2(ListObjectsV2Request.builder()
                        .bucket(bucketName)
                        .build());
                for (S3Object object : objectsResponse.contents()) {
                    s3.deleteObject(DeleteObjectRequest.builder()
                            .bucket(bucketName)
                            .key(object.key())
                            .build());
                    System.out.println("Deleted object: " + object.key() + " from bucket: " + bucketName);
                }
            }
        } catch (S3Exception e) {
            System.err.println("Error cleaning S3 buckets: " + e.getMessage());
        }
    }
}

// mvn exec:java -Dexec.mainClass="local.assignment.AwsCleanup"

