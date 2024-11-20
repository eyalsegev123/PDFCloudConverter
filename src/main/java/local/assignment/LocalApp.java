package local.assignment;

import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceStateName;
import software.amazon.awssdk.services.ec2.model.StartInstancesRequest;
import software.amazon.awssdk.services.ec2.model.StartInstancesResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.File;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.concurrent.TimeUnit;



public class LocalApp {
    
    protected AWS aws = AWS.getInstance();
    protected int localAppCounter = aws.getLocalAppCounter();
    protected String local2ManagerUrl = aws.createQueue("local2Manager");
    protected String manager2LocalUrl = aws.createQueue("manager2Local");
    
    private File getAndCheckInputFile(String inputFileName) {
        File inputFile = new File(inputFileName);

        //Checking that the file is Valid
        if (!inputFile.exists() || !inputFile.isFile() || !inputFile.canRead()) {
            System.out.println("File is not valid");
            return null;
        }
        return inputFile;
    }


    private void checkAndRunManager() {
        List<Instance> managerInstances = aws.getAllInstancesWithLabel(AWS.Label.Manager);
        System.out.println("Checking for active Manager node...");
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
    }


    public void uploadToS3(String inputFileName, File inputFile){
        if(!aws.checkIfBucketExists(aws.bucketName)){
            aws.createBucket(aws.bucketName);    
        }
        try {  
    
            aws.uploadFileToS3("s3://" + aws.bucketName + "/LocalApp" + localAppCounter + "/inputFiles/" + inputFileName, inputFile);    
        } catch (Exception e) {
            System.out.println("Couldn't upload file to S3");  
        }
    }


    private String waitForSQSMessage() {
        try {
            while (true) {
                // Poll for a message from the SQS queue
                List<Message> messages = aws.getSQSMessagesList(manager2LocalUrl);
                if (!messages.isEmpty()) {
                    Message message = messages.get(0);
                    String resultFileKey = message.body();
                    // Remove the processed message from the queue
                    aws.deleteMessage(manager2LocalUrl, message.receiptHandle());
                    System.out.println("Received processed file location: " + resultFileKey);
                    return resultFileKey;
                }
                else 
                    System.out.println("No message received, waiting...");
                    
                TimeUnit.SECONDS.sleep(5); // Poll every 5 seconds
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;            
        }
    }



    
    public void main(String[] args) {

        // Step 1: Read input file from args[0]
        if(args.length == 0) {
            System.err.println("No file has been given");
            return;
        }
        String inputFileName = args[0];
        File inputFile = getAndCheckInputFile(args[0]);
        if(inputFile == null) {
            System.err.println("File coudln't been open");
            return;
        }
        
        // Step 2: Check if Manager node is active , activates if the manager isn't active
        checkAndRunManager();

        //Step 3: Upload to S3
        uploadToS3(inputFileName , inputFile);
                
        //Step 4: Sending a message to SQS queue, stating the location of the file on S3
        aws.sendSQSMessage("s3://" + aws.bucketName + "/inputFiles/" + inputFileName, local2ManagerUrl);
        
        //Step 5: wait the Manager to finish and when he does, takes the location of output in S3
        String OutputLocationInS3 = waitForSQSMessage(); // manager needs to keep the path format with the counter of the local app
        
        //Step 6: Download the output from S3
        File outputFile = new File("OutputFile.html");
        aws.downloadFileFromS3(OutputLocationInS3, outputFile);
        

        //Step 7: Create the HTML
        
    }
        
}




