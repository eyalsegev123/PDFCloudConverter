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
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class LocalApp {

    protected AWS aws = AWS.getInstance();
    protected String localAppID = generateUniqueID();
    protected String local2ManagerUrl = aws.createQueue("local2Manager"); // Shared queue to send messages to the
                                                                          // manager
    protected String manager2LocalUrl = aws.createQueue("manager2Local" + localAppID); // Queue for receiving the
                                                                                       // summary File location in s3
    protected int countLinesInFile = 0;

    private File getAndCheckInputFile(String inputFileName) {
        File inputFile = new File(inputFileName);
        // Checking that the file is Valid
        if (!inputFile.exists() || !inputFile.isFile() || !inputFile.canRead()) {
            System.out.println("File is not valid");
            return null;
        }
        this.countLinesInFile = countLinesInputFile(inputFile);
        return inputFile;
    }

    private int countLinesInputFile(File inputFile) {
        String fileName = inputFile.getName().toLowerCase();

        // Ensure the file is a text file
        if (!fileName.endsWith(".txt")) {
            System.out.println("Unsupported file type. This method only works with .txt files.");
            return 0;
        }

        // Process the file as a text file
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            int lineCount = 0;
            while (reader.readLine() != null) {
                lineCount++;
            }
            return lineCount;
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Couldn't count the number of lines in the text file.");
            return 0;
        }
    }

    private void checkAndRunManager() {
        List<Instance> managerInstances = aws.getAllInstancesWithLabel(AWS.Label.Manager).stream()
                .filter(instance -> !instance.state().name().equals(InstanceStateName.TERMINATED))
                .collect(Collectors.toList());
        System.out.println("Checking for active Manager node...");

        if (!managerInstances.isEmpty()) {
            Instance manager = managerInstances.get(0);
            if (InstanceStateName.RUNNING.equals(manager.state().name())) // RUNNING
                System.out.println("Active Manager node found: " + manager.instanceId());
            else if (InstanceStateName.STOPPED.equals(manager.state().name())) // STOPPED
                aws.startInstance(manager.instanceId());
            else // TERMINATED
                launchNewManager();
        }
        // DOESN'T EXIST
        else
            launchNewManager();

    }

    private void launchNewManager() {
        System.out.println("No active Manager node found. Launching a new one...");
        RunInstancesResponse response = aws.runInstanceFromAmiWithScript(
                InstanceType.T2_NANO,
                1,
                1,
                "Manager");

        String newInstanceId = response.instances().get(0).instanceId();
        aws.tagInstanceAsManager(newInstanceId);
    }

    //
    public void checkBucketAndUploadToS3(String inputFileName, File inputFile) {
        if (!aws.checkIfBucketExists(aws.bucketName)) {
            aws.createBucket(aws.bucketName);
            System.out.println("You have opened a new bucket: " + aws.bucketName);
        }
        try {
            aws.uploadFileToS3("LocalApp" + localAppID + "/inputFiles/" + inputFileName,
                    inputFile);
        } catch (Exception e) {
            System.out.println("Couldn't upload file to S3");
        }
    }

    private String waitForSQSMessage() {
        try {
            while (true) {
                // Poll for a message from the SQS queue
                List<Message> messages = aws.getSQSMessagesList(manager2LocalUrl, 10, 20);
                if (!messages.isEmpty()) {
                    Message message = messages.get(0);
                    String summaryFileLocationS3 = message.body();
                    // Remove the processed message from the queue
                    aws.deleteMessage(manager2LocalUrl, message.receiptHandle());
                    System.out.println("Received processed file location: " + summaryFileLocationS3);
                    return summaryFileLocationS3;
                } else
                    System.out.println("No message received, waiting...");

                TimeUnit.SECONDS.sleep(5); // Poll every 5 seconds
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    // Generate an HTML file from summary data
    private void generateHtmlFileFromSummary(String summaryFileName, String outputHtmlFileName) {
        try (BufferedReader reader = new BufferedReader(new FileReader(summaryFileName));
                PrintWriter writer = new PrintWriter(new FileWriter(outputHtmlFileName))) {

            // Write HTML header
            writer.println("<!DOCTYPE html>");
            writer.println("<html>");
            writer.println("<head>");
            writer.println("<title>URL Conversion Results</title>");
            writer.println("</head>");
            writer.println("<body>");
            writer.println("<h1>URL Conversion Results</h1>");
            writer.println("<ul>");

            // Read and process each line from the summary file
            String line;
            while ((line = reader.readLine()) != null) {
                // Split by tabs (or any other delimiter, adjust as necessary)
                String[] parts = line.split("\t"); // Splitting by tab character
                if (parts.length >= 3) {
                    String operation = parts[0]; // First element: operation
                    String formerUrl = parts[1]; // Second element: former URL
                    String newUrl = parts[2]; // Third element: new URL

                    // Write the HTML list item
                    writer.printf("<li>%s: <a href=\"%s\">%s</a> -> <a href=\"%s\">%s</a></li>%n",
                            operation, formerUrl, formerUrl, newUrl, newUrl);
                } else {
                    System.err.println("Invalid line format in summary file: " + line);
                }
            }

            // Write HTML footer
            writer.println("</ul>");
            writer.println("</body>");
            writer.println("</html>");

        } catch (IOException e) {
            System.err.println("Error generating HTML file from summary: " + e.getMessage());
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
        for (int i = 0; i < length - timePrefix.length(); i++) {
            randomDigits.append(random.nextInt(10)); // Append a random digit (0-9)
        }

        // Combine time prefix and random digits to form a unique ID
        return timePrefix + randomDigits.toString();
    }

    public static void main(String[] args) {

        // Check that we got inputFileName , OutputFileName and workerFileRatio=(n)
        if (args.length <= 2) {
            System.err.println("More params required");
            return;
        }

        LocalApp app = new LocalApp();
        // Step 1: Read relevant info from args
        String inputFileName = args[0];
        String htmlFileName = args[1];
        int workerFileRatio = Integer.parseInt(args[2]);
        File inputFile = app.getAndCheckInputFile(args[0]);
        boolean terminateMode = false;
        if (inputFile == null) {
            System.err.println("File couldn't been open");
            return;
        }
        if (args.length > 3 && args[3].equals("[terminate]")) {
            terminateMode = true;
        }

        // Step 2: Check if Manager node is active , activates if the manager isn't
        // active
        app.checkAndRunManager();

        // Step 3: Upload to S3
        app.checkBucketAndUploadToS3(inputFileName, inputFile);

        // Step 4: Sending a message to SQS queue, stating the location of the file on
        // S3
        app.aws.sendSQSMessage("LocalApp" + app.localAppID + "/inputFiles/"
                + inputFileName + "\t" + terminateMode + "\t" + workerFileRatio + "\t" + app.countLinesInFile,
                app.local2ManagerUrl);

        // Step 5: wait for the Manager to finish and when he does, takes the location
        // of output file in S3
        String summaryFileLocationInS3 = app.waitForSQSMessage();

        // Step 6: Download the summaryFile from S3
        String summaryFileName = "SummaryFile_localApp" + app.localAppID + ".txt";
        File summaryFile = new File(summaryFileName);
        app.aws.downloadFileFromS3(summaryFileLocationInS3, summaryFile);

        // Step 7: Create the HTML
        app.generateHtmlFileFromSummary(summaryFileName, htmlFileName);
        System.out.println("HTML file generated successfully: " + htmlFileName);
        if (summaryFile.exists())
            summaryFile.delete();

        // step 8 : closing local queue
        app.aws.deleteQueue(app.manager2LocalUrl);

    }
}
