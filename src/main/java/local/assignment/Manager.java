package local.assignment;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;


public class Manager {
    protected AWS aws = AWS.getInstance();
    protected int activeWorkers = 0;
    protected Object lockActiveWorkers = new Object();
    protected final String localAppsQueueUrl = aws.getQueueUrl("local2Manager");
    protected final String manager2WorkersQueueUrl = aws.getQueueUrl("manager2WorkersQueue");
    protected final String workers2ManagerQueueUrl = aws.getQueueUrl("workers2ManagerQueue");
    protected ThreadPoolExecutor threadPool;
    protected boolean needsToTerminate = false;
    protected int NUMBER_OF_THREADS = 10;
    protected String ami = "" ; 
    protected String script ="";
    //*** */
    // figure out the ami and script
    // *** 

    
        // Listen for tasks in the SQS queue
    protected void listenForTasks() {
        
        List<Message> messages = aws.getSQSMessagesList(localAppsQueueUrl , 1 , 0);

        if (!messages.isEmpty()) {
            Message message = messages.get(0);
            String[] body = message.body().split("\t");  // path    terminate_mode      ratio
            
            needsToTerminate = body[1].equals("true") ? true : false;
            int workFileRatio = Integer.parseInt(body[2]);
            if(needsToTerminate)
                System.out.println("Termination message received.");

            String locationInS3 = body[0];
            
        	// Process S3 file URLs when a new task is received
            threadPool.submit(() -> processAndDivideS3File(locationInS3, workFileRatio));
            

            // Delete message from queue after processing
			aws.deleteMessage(localAppsQueueUrl, message.receiptHandle());
        }
    }



	protected void processAndDivideS3File(String locationInS3File, int workerFileRatio){
        String[] splitLocation = locationInS3File.split("/");
		File s3InputFile = new File(splitLocation[2] + "_" + splitLocation[4]); // localAPP+id _ fileName
		aws.downloadFileFromS3(locationInS3File, s3InputFile);
		int numOfPdfs = 0;

		try (BufferedReader reader = new BufferedReader(new FileReader(s3InputFile))) {
			String line;
            while ((line = reader.readLine()) != null) {
				aws.sendSQSMessage(line ,manager2WorkersQueueUrl);
				numOfPdfs++;
			}
		} catch (IOException e) {
			System.out.println("error in opening file in buffer");
		}
		startWorkers(numOfPdfs, workerFileRatio);
	}

    
    protected void startWorkers(int numOfPdfs, int n) {
        int numWorkersToAdd;
        synchronized(lockActiveWorkers) {
            // Calculate required workers
            numWorkersToAdd = (int) Math.ceil((double) numOfPdfs / n) - activeWorkers;
            
            if (numWorkersToAdd + activeWorkers > 9) {
                System.out.println("Too many workers are active for this application. Limiting to 9.");
                numWorkersToAdd = 9 - activeWorkers;
            }
        
            System.out.println("Starting " + numWorkersToAdd + " workers...");
            activeWorkers += numWorkersToAdd;
        }
        for (int i = 0; i < numWorkersToAdd; i++) {
            aws.runInstanceFromAmiWithScript(ami , InstanceType.T2_NANO , 1 , 1 , script);
        }
    }

    protected void mergeToSummaryAndUpload() {
        //TO DO
    }






    public static void main(String[] args) {
        Manager manager = new Manager();
        manager.threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        System.out.println("Thread pool initialized with: " + manager.NUMBER_OF_THREADS);
        while (true){
        	manager.listenForTasks();           
        }
     
    }
}











//connect between the input url to the output url
        // when the manager gets a message from the localApp - check if its terminate message or an sqs message
        // maybe add stopped mode
    
    // Reads the input file and processes each line based on the operation (ToImage, ToHTML, ToText)
    // public static void processInputFile(String inputFilePath) {
    //     try {
    //         

    //             // Call the appropriate method based on the operation
    //             switch (operation) {
    //                 case "ToImage":
    //                     PDFUtils.ToImage(pdfUrl);
    //                     break;
    //                 case "ToHTML":
    //                     PDFUtils.ToHTML(pdfUrl);
    //                     break;
    //                 case "ToText":
    //                     PDFUtils.ToText(pdfUrl);
    //                     break;
    //                 default:
    //                     System.out.println("Unknown operation: " + operation);
    //             }
    //         }
    //         reader.close();
    //     }
    //     catch(IOException e) {
    //         System.out.println("PDF not found or Invalid"); 
    //         e.printStackTrace();         
    //     }
    // }


    // Split the line by tab
    // String[] parts = line.split("\t");
    // String operation = parts[0];
    // String pdfOriginalUrl = parts[1];