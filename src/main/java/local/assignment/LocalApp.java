package local.assignment;

import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceStateName;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.sqs.model.Message;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;



public class LocalApp {
    
    protected AWS aws = AWS.getInstance();
    protected String localAppID = generateUniqueID();
    protected String local2ManagerUrl = aws.createQueue("local2Manager");
    protected String manager2LocalUrl = aws.createQueue("manager2Local" + localAppID);
    
    private File getAndCheckInputFile(String inputFileName) {
        File inputFile = new File(inputFileName);

        //Checking that the file is Valid
        if (!inputFile.exists() || !inputFile.isFile() || !inputFile.canRead()) {
            System.out.println("File is not valid");
            return null;
        }
        return inputFile;
    }

    private String checkAndRunManager() {
        List<Instance> managerInstances = aws.getAllInstancesWithLabel(AWS.Label.Manager).stream()
                                            .filter(instance -> !instance.state().name().equals(InstanceStateName.TERMINATED))
                                            .collect(Collectors.toList());
        System.out.println("Checking for active Manager node...");  
        
        if (!managerInstances.isEmpty()) {
            Instance manager = managerInstances.get(0);
            if (InstanceStateName.RUNNING.equals(manager.state().name()))  //RUNNING
                System.out.println("Active Manager node found: " + manager.instanceId());
            else  //STOPPED
                aws.startInstance(manager.instanceId());
            return manager.instanceId();
        } 
        //DOESN'T EXIST
        else 
            return launchNewManager();
        
    }
    
    private String launchNewManager() {
        System.out.println("No active Manager node found. Launching a new one...");
        String userDataScript = "";  // Validate or define your script here

        RunInstancesResponse response = aws.runInstanceFromAmiWithScript(
                aws.IMAGE_AMI,
                InstanceType.T2_NANO,
                1,
                1,
                userDataScript
        );

        String newInstanceId = response.instances().get(0).instanceId();
        aws.tagInstanceAsManager(newInstanceId);
        return newInstanceId;
    }

    public void uploadToS3(String inputFileName, File inputFile){
        if(!aws.checkIfBucketExists(aws.bucketName)){
            aws.createBucket(aws.bucketName);
            System.out.println("You have opened a new bucket: " + aws.bucketName);    
        }
        try {  
            aws.uploadFileToS3("s3:/" + aws.bucketName + "/LocalApp" + localAppID + "/inputFiles/" + inputFileName, inputFile);    
        } catch (Exception e) {
            System.out.println("Couldn't upload file to S3");  
        }
    }

    private String waitForSQSMessage() {
        try {
            while (true) {
                // Poll for a message from the SQS queue
                List<Message> messages = aws.getSQSMessagesList(manager2LocalUrl , 10 , 20);
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

    private void mergeFilesToOutput(String summaryFileName, String inputFileName, String HtmlFileName){
        try {
            // Read the input file (operation and former URL)
            List<String[]> inputData = readInputFile(inputFileName);

            // Read the S3 summary file (new URLs)
            List<String> s3Data = readS3File(summaryFileName);

            if (inputData.size() != s3Data.size()) {
                System.err.println("Mismatch between input file lines and S3 file lines.");
                return;
            }

            // Generate HTML content
            generateHtmlFile(inputData, s3Data, HtmlFileName);

            System.out.println("HTML file generated successfully: " + HtmlFileName);

        }
    catch (IOException e) {
            System.err.println("Error processing files while merging: " + e.getMessage());
    }
}

    //return list of operation and the corresponding url
    private List<String[]> readInputFile(String inputFileName) throws IOException {
        List<String[]> inputData = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFileName))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\s+");
                if (parts.length == 2) {
                    inputData.add(parts); // [operation, formerUrl]
                } else {
                    System.err.println("Invalid input line: " + line);
                }
            }
        }
        return inputData;
    }

    private List<String> readS3File(String s3FileName) throws IOException {
        List<String> s3Data = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(s3FileName))) {
            String line;
            while ((line = reader.readLine()) != null) {
                s3Data.add(line.trim()); // Add new URL
            }
        }
        return s3Data;
    }

    private void generateHtmlFile(List<String[]> inputData, List<String> s3Data, String outputHtmlFileName) throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(outputHtmlFileName))) {
            // Write HTML header
            writer.println("<!DOCTYPE html>");
            writer.println("<html>");
            writer.println("<head>");
            writer.println("<title>URL Conversion Results</title>");
            writer.println("</head>");
            writer.println("<body>");
            writer.println("<h1>URL Conversion Results</h1>");
            writer.println("<ul>");

            // Add each line to the HTML
            for (int i = 0; i < inputData.size(); i++) {
                String operation = inputData.get(i)[0];
                String formerUrl = inputData.get(i)[1];
                String newUrl = s3Data.get(i);

                writer.printf("<li>%s: <a href=\"%s\">%s</a> -> <a href=\"%s\">%s</a></li>%n",
                        operation, formerUrl, formerUrl, newUrl, newUrl);
            }

            // Write HTML footer
            writer.println("</ul>");
            writer.println("</body>");
            writer.println("</html>");
        }
    }

    public static String generateUniqueID() {
        Random random = new Random();
        
        // Generate a random length between 5 and 10
        int length = 5 + random.nextInt(6);
        
        // Generate a unique prefix based on current time in milliseconds
        String timePrefix = String.valueOf(System.currentTimeMillis()).substring(6, 13);
        
        // Generate random digits to fill the rest
        StringBuilder randomDigits = new StringBuilder();
        for (int i = 0; i < length; i++) {
            randomDigits.append(random.nextInt(10)); // Append a random digit (0-9)
        }
    
        // Combine time prefix and random digits to form a unique ID
        return timePrefix + randomDigits.toString().substring(0, length - timePrefix.length());
    }
    
    public static void main(String[] args) {
        
        if(args.length <= 2) {
            System.err.println("More params required");
            return;
        }
        
        LocalApp app = new LocalApp();
        // Step 1: Read relevant info from args
        String inputFileName = args[0];
        String HtmlFileName = args[1];
        int workerFileRatio = Integer.parseInt(args[2]);
        File inputFile = app.getAndCheckInputFile(args[0]);
        boolean terminateMode = false;
        if(inputFile == null) {
            System.err.println("File couldn't been open");
            return;
        }
        if(args.length > 3 && args[3].equals("[terminate]")) {
            terminateMode = true; 
        }
        
        // Step 2: Check if Manager node is active , activates if the manager isn't active
        String managerId = app.checkAndRunManager();

        //Step 3: Upload to S3
        app.uploadToS3(inputFileName , inputFile);
                
        //Step 4: Sending a message to SQS queue, stating the location of the file on S3
        app.aws.sendSQSMessage("s3:/" + app.aws.bucketName + "/LocalApp"  + app.localAppID + "/inputFiles/" + inputFileName + "\t" + terminateMode + "\t" + workerFileRatio , app.local2ManagerUrl);
        
        //Step 5: wait for the Manager to finish and when he does, takes the location of output file in S3
        String OutputLocationInS3 = app.waitForSQSMessage(); // manager needs to keep the path format with the counter of the local app
        
        //Step 6: Download the summaryFile from S3 , Assuming the files are coming back in same order as input 
        String summaryFileName = "SummaryFile_localApp" + app.localAppID + ".txt";
        File summaryFile = new File(summaryFileName);
        app.aws.downloadFileFromS3(OutputLocationInS3, summaryFile);

        //Step 7: Create the HTML
        app.mergeFilesToOutput(summaryFileName, inputFileName, HtmlFileName);
        
        // Step 8: checks terminate mode
        if(terminateMode) {
            //send a message to sqs indicating the manager needs to terminate 
            app.aws.sendSQSMessage("terminate", app.local2ManagerUrl);
        }

        // step 9 : closing local queue
        app.aws.deleteQueue(app.manager2LocalUrl);
        
    }
}





