package local.assignment;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
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
            while(!messages.isEmpty()) {
                //message format: operation   originalUrl     targetLocationInS3      inputFileName_index (seperated by tabs) 
                //targetLocation in S3 -->  s3:/localapp123/outputFiles/
                String[] splitMessage = messages.get(0).body().split("\t");
                String operation = splitMessage[0];
                String originalUrl = splitMessage[1];
                String targetlLocationInS3 = splitMessage[2];
                String fileNameToUploadWithIndex = splitMessage[3];

                File fileAfterOperation;

                switch (operation) {
                    case "ToHTML":
                        fileNameToUploadWithIndex += ".html";
                        fileAfterOperation = PDFUtils.ToHTML(originalUrl, fileNameToUploadWithIndex);
                        break;

                    case "ToImage":
                        fileNameToUploadWithIndex += ".png";
                        fileAfterOperation = PDFUtils.ToImage(targetlLocationInS3, fileNameToUploadWithIndex);
                        break;

                    case "ToText":
                        fileNameToUploadWithIndex += ".txt";
                        fileAfterOperation = PDFUtils.ToText(targetlLocationInS3, fileNameToUploadWithIndex);
                        break;
                    
                    default:
                        fileAfterOperation = null;
                        System.out.println("invalid operation , couldn't convert file");
                        break;
                }
                
                //upload each subFFile to S3 with appropiate index and extension
                try {
                    worker.aws.uploadFileToS3(targetlLocationInS3, fileAfterOperation);
                }
                catch(Exception e) {
                    System.out.println("Exception occured: couldn't upload file to S3");
                }
                
                //Send SQS message to the manager
                worker.aws.sendSQSMessage(targetlLocationInS3 + "/" + fileNameToUploadWithIndex , worker.workers2ManagerQueueUrl);
            }
        } 

    }
}