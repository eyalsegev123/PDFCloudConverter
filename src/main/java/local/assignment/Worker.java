package local.assignment;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import local.assignment.PDFUtils;
import software.amazon.awssdk.services.sqs.model.Message;

public class Worker {
    protected AWS aws = AWS.getInstance();
    protected final String manager2WorkersQueueUrl = aws.getQueueUrl("manager2WorkersQueue");
    protected final String workers2ManagerQueueUrl = aws.getQueueUrl("workers2ManagerQueue");

    
    // Fetch and process messages from the queue
    private void processMessages() {
        List<Message> messages = aws.getSQSMessagesList(manager2WorkersQueueUrl, 1, 10);
        if (!messages.isEmpty()) {
            Message message = messages.get(0);
            handleMessage(message);
        }
    }

    // Handle individual messages
    private void handleMessage(Message message) {
        String[] splitMessage = message.body().split("\t");
        String operation = splitMessage[0];
        String originalUrl = splitMessage[1];
        String targetLocationInS3 = splitMessage[2];
        String fileNameToUploadWithIndex = splitMessage[3] + "_" + splitMessage[4];

        try {
            File fileAfterOperation = performOperation(operation, originalUrl, fileNameToUploadWithIndex);
            uploadAndNotify(fileAfterOperation, targetLocationInS3, fileNameToUploadWithIndex);
        } catch (Exception e) {
            System.out.println("Error processing message: " + e.getMessage());
        } finally {
            aws.deleteMessage(manager2WorkersQueueUrl, message.receiptHandle());
        }
    }

    // Perform the specified operation
    private File performOperation(String operation, String originalUrl, String fileName) throws Exception {
        File file = null;
        Exception exc = null;

        switch (operation) {
            case "ToHTML":
                file = new File(fileName + ".html");
                exc = PDFUtils.ToHTML(originalUrl, file);
                break;
            case "ToImage":
                file = new File(fileName + ".png");
                exc = PDFUtils.ToImage(originalUrl, file);
                break;
            case "ToText":
                file = new File(fileName + ".txt");
                exc = PDFUtils.ToText(originalUrl, file);
                break;
            default:
                throw new IllegalArgumentException("Invalid operation: couldn't convert file");
        }

        if (exc != null) {
            return handleError(exc, fileName);
        }
        return file;
    }

    // Handle errors and create an error file
    private File handleError(Exception exc, String fileName) throws IOException {
        String errorFileName = fileName + "_Error.txt";
        File errorFile = new File(errorFileName);
        try (FileWriter writer = new FileWriter(errorFile)) {
            writer.write("Error Message: " + exc.getMessage());
        }
        return errorFile;
    }

    // Upload file to S3 and notify the manager
    private void uploadAndNotify(File file, String targetLocationInS3, String fileNameToUploadWithIndex) {
        try {
            aws.uploadFileToS3(targetLocationInS3, file);
            aws.sendSQSMessage(targetLocationInS3 + fileNameToUploadWithIndex, workers2ManagerQueueUrl);
            if (file.exists()) 
                file.delete();
        } catch (Exception e) {
            System.out.println("Failed to upload or notify: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        Worker worker = new Worker();
        while (true) 
            worker.processMessages();
    }
}
