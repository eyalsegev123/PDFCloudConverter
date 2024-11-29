package local.assignment;

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
                //message format: operation   originalUrl     index     targetLocationInS3 (seperated by tabs)
                //targetLocation in S3 -->  s3:/localapp123/outputFiles/haguvi.txt

                String[] splitMessage = messages.get(0).body().split("\t");
                String operation = splitMessage[0];
                String originalUrl = splitMessage[1];
                String index = splitMessage[2];
                String locationInS3 = splitMessage[3];
            
            }
        } 
        
    

}}