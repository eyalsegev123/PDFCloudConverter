package local.assignment;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import software.amazon.awssdk.services.sqs.model.Message;

public class Worker {
    // look how we sent the message to the worker -we should resend the manager the index
    // when the worker uploads a file to s3 it should upload it with its correct index for the merge;
    protected AWS aws = AWS.getInstance();
    protected final String manager2WorkersQueueUrl = aws.getQueueUrl("manager2WorkersQueue");
    protected final String workers2ManagerQueueUrl = aws.getQueueUrl("workers2ManagerQueue");


    public static void main(String[] args) {
        Worker worker = new Worker();
        while(true) {
            List<Message> messages = worker.aws.getSQSMessagesList(worker.manager2WorkersQueueUrl , 1,10);
            if(!messages.isEmpty()) {
                //message format: operation   originalUrl     targetLocationInS3    inputFileName  index (seperated by tabs) 
                //targetLocation in S3 -->  s3:/localapp123/outputFiles/
                Message message = messages.get(0);
                String[] splitMessage = message.body().split("\t");
                String operation = splitMessage[0];
                String originalUrl = splitMessage[1];
                String targetlLocationInS3 = splitMessage[2];
                String fileNameToUploadWithIndex = splitMessage[3] + "_" + splitMessage[4];//---> input-sample-1_1

                File fileAfterOperation;
                Exception exc = null;

                switch (operation) {
                    case "ToHTML":
                        fileNameToUploadWithIndex += ".html";
                        fileAfterOperation = new File(fileNameToUploadWithIndex);
                        exc = PDFUtils.ToHTML(originalUrl, fileAfterOperation);
                        break;

                    case "ToImage":
                        fileNameToUploadWithIndex += ".png";
                        fileAfterOperation = new File(fileNameToUploadWithIndex);
                        exc = PDFUtils.ToImage(targetlLocationInS3, fileAfterOperation);
                        break;

                    case "ToText":
                        fileNameToUploadWithIndex += ".txt";
                        fileAfterOperation = new File(fileNameToUploadWithIndex);
                        exc = PDFUtils.ToText(targetlLocationInS3, fileAfterOperation);
                        break;
                    
                    default:
                        fileAfterOperation = null;
                        System.out.println("invalid operation , couldn't convert file");
                        break;
                }
                

                if (exc != null) { //Error - an exception has occured in the operation process
                    
                    //Check if files Exists and Delete it 
                    if (fileAfterOperation.exists())
                        fileAfterOperation.delete();
                    
                    int dotIndex = fileNameToUploadWithIndex.lastIndexOf(".");
                    fileNameToUploadWithIndex = fileNameToUploadWithIndex.substring(0, dotIndex)  + "_Error.txt"; //input-sample-1_index_Error.txt

                    // Generate an error file with the same index but a distinct marker
                    fileAfterOperation = new File(fileNameToUploadWithIndex);
                        
                    try (FileWriter writer = new FileWriter(fileAfterOperation)) {
                        writer.write("Error Message: " + exc.getMessage()); // Add more details if available
                    } catch (IOException e) {
                        System.out.println("Failed to create error file: " + e.getMessage());
                        return; // Stop processing if the error file can't be created
                    }
                }

                //Upload each subFile to S3 with appropiate index and extension (Also taking care of Errors)
                try {
                    worker.aws.uploadFileToS3(targetlLocationInS3, fileAfterOperation);
                    //Send SQS message to the manager
                    worker.aws.sendSQSMessage(targetlLocationInS3 + fileNameToUploadWithIndex , worker.workers2ManagerQueueUrl);
                    if(fileAfterOperation.exists())
                        fileAfterOperation.delete();
                }
                catch(Exception e) {
                    System.out.println("Exception occured: couldn't upload file to S3");
                }
                worker.aws.deleteMessage(worker.manager2WorkersQueueUrl, message.receiptHandle());    
            }
            
        } 

    }
}