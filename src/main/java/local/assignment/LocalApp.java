package local.assignment;

import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceStateName;
import software.amazon.awssdk.services.ec2.model.StartInstancesRequest;
import software.amazon.awssdk.services.ec2.model.StartInstancesResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.File;
import java.util.List;

public class LocalApp {
    
    protected AWS aws = new AWS();
    protected String localSQSUrl = aws.createQueue("Local2Manager");
    
    public void main(String[] args) {

        // Step 1: Read input file from args[0]
        if(args.length == 0) {
            System.err.println("No file has been given");
            return;
        }
        String inputFileName = args[0];
        File inputFile = new File(inputFileName);

        //Checking that the file is Valid
        if (!inputFile.exists() || !inputFile.isFile() || !inputFile.canRead()) {
            System.out.println("File is not valid");
            return;
        }
        
        
        // Step 2: Check if Manager node is active
        System.out.println("Checking for active Manager node...");
        List<Instance> managerInstances = aws.getAllInstancesWithLabel(AWS.Label.Manager);
        
        if (managerInstances.size() > 0) {
            Instance manager = managerInstances.get(0);
            if(manager.state().name().equals(InstanceStateName.RUNNING)) {
                System.out.println("Active Manager node found: " + manager.instanceId());
            }
            else { //manager instance is not RUNNING
                StartInstancesRequest startRequest = StartInstancesRequest.builder()
                    .instanceIds(manager.instanceId())
                    .build();

                // Start the instance
                StartInstancesResponse response = aws.ec2.startInstances(startRequest);
                System.out.println("Active Manager node found and activated: " + managerInstances.get(0).instanceId());
            }
            
        } 
        else { //list size == 0
            // Define the user data script as a String
            String userDataScript = ""; //Figure out the script 
            aws.runInstanceFromAmiWithScript(
                        aws.IMAGE_AMI,            // AMI ID
                        software.amazon.awssdk.services.ec2.model.InstanceType.T2_NANO, // Instance type
                        1,                        // Minimum count
                        1,                        // Maximum count
                        userDataScript            // User data script
            );
                
            System.out.println("No active Manager node found. Launching a new one..."); 
        }

        //Step 3: Upload to S3
        if(!aws.checkIfBucketExists(aws.bucketName)){
            aws.createBucket(aws.bucketName);    
        }
        try {  

            aws.uploadFileToS3("s3://" + aws.bucketName + "/inputFiles/" + inputFileName, inputFile);    
        } catch (Exception e) {
            System.out.println("Couldn't upload file to S3");  
        }

        
        //Step 4: Sending a message to SQS queue, stating the location of the file on S3
        sendSQSMessage("s3://" + aws.bucketName + "/inputFiles/" + inputFileName);

        
        
    }

    
    private void sendSQSMessage(String messageBody) {
        SendMessageRequest request = SendMessageRequest.builder()
            .queueUrl(localSQSUrl)
            .messageBody(messageBody)
            .build();

        aws.sqs.sendMessage(request);
        System.out.println("Message sent to queue: " + localSQSUrl + "/nMessage Sent: " + messageBody);
    }

    
}  

