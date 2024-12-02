package local.assignment;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceStateName;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.sqs.model.Message;


public class Manager {
    protected AWS aws = AWS.getInstance();
    protected int activeWorkers = 0;
    protected Object lockActiveWorkers = new Object();
    protected final String localAppsQueueUrl = aws.getQueueUrl("local2Manager");
    protected final String manager2WorkersQueueUrl = aws.createQueue("manager2WorkersQueue");
    protected final String workers2ManagerQueueUrl = aws.createQueue("workers2ManagerQueue");
    protected ThreadPoolExecutor threadPool;
    protected boolean needsToTerminate = false;
    protected int NUMBER_OF_THREADS = 5;
    protected HashMap<String , Integer> locationToCountTarget = new HashMap<>(); // locations in s3 :: number of files we need to edit
    protected HashMap<String , Integer> locationToCurrentCounter = new HashMap<>(); // locations in s3 :: number of files we already edited
    protected int totalLocalApps = 0;
    //*** */
    // figure out the ami and script
    // *** 

    
        // Listen for tasks in the SQS queue
    protected void listenForTasksLocal() {
        while (true) {
            List<Message> messages = aws.getSQSMessagesList(localAppsQueueUrl , 1 , 10);
            if (!messages.isEmpty()) {
                Message message = messages.get(0);
                String[] body = message.body().split("\t");  // PathToInputFileInS3   terminate_mode     ratio   countLines
                
                needsToTerminate = body[1].equals("true") ? true : false;
                int workFileRatio = Integer.parseInt(body[2]);
                int countLines = Integer.parseInt(body[3]);
                
                // s3:/localapp123/inputFiles/input-sample-1.txt ----> s3:/localapp123/outputFiles/input-sample-1.txt
                String targetLocationInS3 = body[0].replace("/inputFiles/", "/outputFiles/");
                locationToCountTarget.put(targetLocationInS3, countLines);
                locationToCurrentCounter.put(targetLocationInS3, 0);
                
                // Process S3 file URLs when a new task is received
                threadPool.submit(() -> processAndDivideS3File(body[0], workFileRatio));
                // Delete message from queue after processing
                aws.deleteMessage(localAppsQueueUrl, message.receiptHandle());
                if(needsToTerminate){
                    //Retreives the number of total localApps that were sent until terminate message
                    totalLocalApps = locationToCountTarget.size();
                    System.out.println("Submitted last file & Termination message received.");
                    break;
                }
            }    
        }
        System.out.println("listener of LocalApps finished his job");   
    }

	protected void processAndDivideS3File(String inputFileLocationInS3, int workerFileRatio){
        
        String[] splitLocation = inputFileLocationInS3.split("/");
        //Download file from S3 
		File s3InputFile = new File(splitLocation[2] + "_" + splitLocation[4]); // localApp+ID_fileName
		aws.downloadFileFromS3(inputFileLocationInS3, s3InputFile);
		int indexOfCurrentPDF = 0;
        
        // Extract the base name
        String inputFileNameWithExt = splitLocation[4]; //input-sample-1.txt
        int dotIndex = inputFileNameWithExt.lastIndexOf(".");
        String inputFileNameWithoutExt = inputFileNameWithExt.substring(0, dotIndex) ; //input-sample-1 (without extension)

        //Cut the targetLocationInS3 to be without the name of the input-file
        int lastSlashIndex = inputFileLocationInS3.lastIndexOf("/");
        String targetLocationInS3 = inputFileLocationInS3.substring(0, lastSlashIndex+1).replace("/inputFiles/", "/outputFiles/");
        
        //Send a message for each subFile in the input-file
		try (BufferedReader reader = new BufferedReader(new FileReader(s3InputFile))) {
			String line;
            while ((line = reader.readLine()) != null) {
                // line=(operation   originalUrl)   targetLocation     inputFileName_index (seperated by tabs) 
				aws.sendSQSMessage(line + "\t" + targetLocationInS3 + "\t" + inputFileNameWithoutExt + "\t" + indexOfCurrentPDF ,manager2WorkersQueueUrl);
                indexOfCurrentPDF++;
			}
		} catch (IOException e) {
			System.out.println("error in opening file in buffer");
		}
        if(s3InputFile.exists()){
            s3InputFile.delete();
        }
		activateWorkers(indexOfCurrentPDF , workerFileRatio);
	}
    
    protected void activateWorkers(int numOfPdfs, int workerFileRatio) {
        int numWorkersToAdd;
        synchronized(lockActiveWorkers) {
            // Calculate required workers
            numWorkersToAdd = (int) Math.ceil((double) numOfPdfs / workerFileRatio) - activeWorkers;
            
            if (numWorkersToAdd + activeWorkers > 9) {
                System.out.println("Too many workers are active for this application. Limiting to 9.");
                numWorkersToAdd = 9 - activeWorkers;
            }
        
            System.out.println("Starting " + numWorkersToAdd + " workers...");
            activeWorkers += numWorkersToAdd;
        }
        for (int i = 0; i < numWorkersToAdd; i++) {
            RunInstancesResponse response = aws.runInstanceFromAmiWithScript(InstanceType.T2_NANO , 1 , 1 , "Worker");
            String newInstanceId = response.instances().get(0).instanceId();
            aws.tagInstanceAsWorker(newInstanceId);
        }
    }

    protected void terminateThreadPool(){
        threadPool.shutdown();  // Initiates an orderly shutdown
        System.out.println("Started shutdown of ThreadPool");
        try {
            
            if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();  // Force shutdown if tasks take too long
            }
        } 
        catch (InterruptedException e) {
            threadPool.shutdownNow();  // Handle interruption
            Thread.currentThread().interrupt();
        }
    }

    protected void listenForWorkersTasks() {
        int numberOfAppsSubmitted = 0;
        while (true) {
            //The worker sends location of the URL in S3
            List<Message> messages = aws.getSQSMessagesList(workers2ManagerQueueUrl , 5 , 2);
            while (!messages.isEmpty()) {
                Message message = messages.get(0);
                messages.remove(0);
                String locationInS3 = message.body();  //newFilePathInS3(_index in the name)  
                int lastCounter = locationToCurrentCounter.get(locationInS3); // the current amount of files already edited
                int updatedCounter = lastCounter++;
                locationToCountTarget.put(locationInS3 , lastCounter);
                if(updatedCounter == locationToCountTarget.get(locationInS3)) {
                    // Extract the substring up to the last '/' ---> s3:/bucket-name/LocalApp+ID/outputFiles/malawach.txt ---> s3:/bucket-name/LocalApp+ID/outputFiles/
                    int lastSlashIndex = locationInS3.lastIndexOf('/');
                    String outputFolderPath = locationInS3.substring(0, lastSlashIndex + 1);
                    threadPool.submit(() -> mergeToSummaryAndUploadToS3(outputFolderPath));
                    numberOfAppsSubmitted++;                    
                }
                aws.deleteMessage(workers2ManagerQueueUrl, message.receiptHandle());       
            }
            if(numberOfAppsSubmitted == totalLocalApps) {
                // All files have been submitted to merge task --> meaning no new missions to submit
                terminateThreadPool();
                System.out.println("Thread Pool finishes its last tasks and will be terminated");
                break;
            }
        }
        System.out.println("listener for Workers finished his job");
    }

    protected void mergeToSummaryAndUploadToS3(String outputFolderPath) {
        try {  
            List<String> filesToMerge = aws.getFilesInFolder(outputFolderPath);
            //any file in the list will be represented like this:  LocalApp123/outputFiles/ 
            Collections.sort(filesToMerge);
            File LocalAppInputFile = new File("LocalAppInputFile.txt");
            List<String> inputFileList = aws.getFilesInFolder(outputFolderPath.replace("/outputFiles/", "/inputFiles/"));
            aws.downloadFileFromS3(inputFileList.get(0), LocalAppInputFile);
            File summaryFile = new File("summary.txt");


            // Remove the 's3://bucket/' part from the path
            String path = outputFolderPath.substring(outputFolderPath.indexOf("LocalApp"));

            // Extract the part after "LocalApp"
            String localAppId = path.substring("LocalApp".length(), path.indexOf("/", "LocalApp".length()));

            // Open the summary file for writing
            BufferedWriter writer = new BufferedWriter(new FileWriter(summaryFile));
            // Open the input file for reading
            BufferedReader inputReader = new BufferedReader(new FileReader(LocalAppInputFile));
            String inputLine;
            
            // Iterate over each file in filesToMerge
            for (String newFileUrl : filesToMerge) {
                // Read the next line from the input file
                inputLine = inputReader.readLine();

                //Parse the newFileUrl and understand if there is Error
                int lastSlashIndex = newFileUrl.lastIndexOf("/");
                String fileNameWithIndex = newFileUrl.substring(lastSlashIndex + 1, newFileUrl.length());
                boolean isError = fileNameWithIndex.contains("Error");
                
                // If there's no more lines in input file, break the loop
                if (inputLine == null) {
                    break;
                }
                
                // Write the line from the input file along with the file URL into summary.txt, Or add the Error Description if occured
                if (isError) {
                    // Download and read the error file content
                    File errorFile = new File("errorFile.txt");
                    aws.downloadFileFromS3(newFileUrl, errorFile);  // Download the error file from S3
            
                    // Read the error description
                    StringBuilder errorDescription = new StringBuilder();
                    try (BufferedReader errorReader = new BufferedReader(new FileReader(errorFile))) {
                        String errorLine;
                        while ((errorLine = errorReader.readLine()) != null) {
                            errorDescription.append(errorLine).append(" ");  // Collect all lines from the error file (containing Error message)
                        }
                    }
            
                    // Write the input line along with the formatted error details
                    //Example: InputLine1 =(operation originalUrl)   Error Message: File not found
                    writer.write(inputLine + "\t" + errorDescription.toString().trim());
                    if(errorFile.exists())
                        errorFile.delete();
                } else {
                    // Normal case: write the input line with the file URL
                    writer.write(inputLine + "\t" + newFileUrl);
                }
                writer.newLine(); // New line for the next entry
            }
            
            //Close resources streams and inputFile
            inputReader.close();
            writer.close();
            if(LocalAppInputFile.exists()){
                LocalAppInputFile.delete();
            }

            // Upload the summary file to S3
            aws.uploadFileToS3(outputFolderPath + "summaryFile/", summaryFile);
            aws.sendSQSMessage(outputFolderPath + "summaryFile/", aws.getQueueUrl("manager2Local" + localAppId));    
        }
        catch (Exception e) {
            e.printStackTrace();
            // Handle exceptions here
        }
    }

    public void deActivateEC2Nodes() {
        List<Instance> workers = aws.getAllInstancesWithLabel(AWS.Label.Worker);
        for(Instance workerInstance : workers) {
            if(workerInstance.state().name() == InstanceStateName.RUNNING) {
                aws.terminateInstance(workerInstance.instanceId());
                // terminate running workers
            }
        }
        System.out.println("Terminated all Workers");
        
        List<Instance> managers = aws.getAllInstancesWithLabel(AWS.Label.Manager);
        for(Instance managerInstance : managers) {
            if(managerInstance.state().name() == InstanceStateName.RUNNING) {
                aws.terminateInstance(managerInstance.instanceId());
                // terminate running manager
            }
        }
        System.out.println("Terminated Manager");
    }



    public static void main(String[] args) {
        Manager manager = new Manager();
        manager.threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(manager.NUMBER_OF_THREADS);
        System.out.println("Thread pool initialized with: " + manager.NUMBER_OF_THREADS);
        
        Thread localListener = new Thread(() -> manager.listenForTasksLocal());
        Thread workersListener = new Thread(() -> manager.listenForWorkersTasks());
        
        localListener.start();
        workersListener.start();
    
        System.out.println("Main Thread: All threads have started, waiting...");
        
        try {
            localListener.join();
            workersListener.join();
        } catch (InterruptedException e) {
            System.err.println("Main Thread: Interrupted while waiting for threads to complete.");
            Thread.currentThread().interrupt(); // Preserve interrupt status
        }
        System.out.println("Main Thread: All threads have completed execution. Shutting down EC2 instances.");
        manager.deActivateEC2Nodes();
    }
}