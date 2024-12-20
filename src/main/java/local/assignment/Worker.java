package local.assignment;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import software.amazon.awssdk.services.sqs.model.Message;

public class Worker {
    protected AWS aws = AWS.getInstance();
    protected final String manager2WorkersQueueUrl = aws.getQueueUrl("manager2WorkersQueue");
    protected final String workers2ManagerQueueUrl = aws.getQueueUrl("workers2ManagerQueue");

    // Fetch and process messages from the queue
    private void processMessages() {
        System.out
                .println(Thread.currentThread() + "Hi im a worker I'm trying to get messages from manager2WorkersQueue");
        List<Message> messages = aws.getSQSMessagesList(manager2WorkersQueueUrl, 1, 10);
        if (!messages.isEmpty()) {
            Message message = messages.get(0);
            handleMessage(message);
        }
    }

    // Handle individual messages
    private void handleMessage(Message message) {
        System.out.println(Thread.currentThread() + ": handling the message");
        String[] splitMessage = message.body().split("\t");
        String operation = splitMessage[0];
        String originalUrl = splitMessage[1];
        String targetLocationInS3 = splitMessage[2];
        String fileNameToUploadWithIndex = splitMessage[3] + "_" + splitMessage[4];
        System.out.println("operation: " + operation);
        System.out.println("originalUrl: " + originalUrl);
        System.out.println("targetLocationInS3: " + targetLocationInS3);
        System.out.println("fileNameToUploadWithIndex: " + fileNameToUploadWithIndex);
    
        try {
            File fileAfterOperation = performOperation(operation, originalUrl, fileNameToUploadWithIndex);
            uploadAndNotify(fileAfterOperation, targetLocationInS3, fileAfterOperation.getName());
            if (fileAfterOperation.exists())
                fileAfterOperation.delete();
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
        System.out.println("Starting to perform: " + operation + "On file: " + fileName);

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
        System.out.println(Thread.currentThread() + " Handling error on file: " + fileName);
        System.out.println(exc.getMessage());
        int lastIndex = fileName.lastIndexOf("_");
        String errorFileName = fileName.substring(0, lastIndex) + "_Error" + fileName.substring(lastIndex) + ".txt";
        File errorFile = new File(errorFileName);
        try (FileWriter writer = new FileWriter(errorFile)) {
            writer.write("Error Message: " + exc.getMessage());
        }
        return errorFile;
    }

    // Upload file to S3 and notify the manager
    private void uploadAndNotify(File file, String targetLocationInS3, String fileNameToUploadWithIndex) {
        try {
            aws.uploadFileToS3(targetLocationInS3 + fileNameToUploadWithIndex, file);
            aws.sendSQSMessage(targetLocationInS3 + fileNameToUploadWithIndex, workers2ManagerQueueUrl);
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
