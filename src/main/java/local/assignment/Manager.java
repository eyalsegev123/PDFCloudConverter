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
    protected HashMap<String, Integer> locationToCountTarget = new HashMap<>(); // locations in s3 :: number of files we
    // need to edit
    protected HashMap<String, Integer> locationToCurrentCounter = new HashMap<>(); // locations in s3 :: number of files
    // we already edited
    protected int totalLocalApps = -1;
    // *** */
    // figure out the ami and script
    // ***
    
    // Listen for tasks in the SQS queue
    protected void listenForTasksLocal() {
        System.out.println(Thread.currentThread() + "Trying to get local app message");
        while (true) {
            List<Message> messages = aws.getSQSMessagesList(localAppsQueueUrl, 1, 10);
            if (!messages.isEmpty()) {
                Message message = messages.get(0);
                String[] body = message.body().split("\t"); // PathToInputFileInS3 terminate_mode ratio countLines

                needsToTerminate = body[1].equals("true") ? true : false;
                int workFileRatio = Integer.parseInt(body[2]);
                int countLines = Integer.parseInt(body[3]);

                // Localapp123/inputFiles/input-sample-1.txt ---->
                // Localapp123/outputFiles/input-sample-1.txt
                String targetLocationInS3 = body[0].replace("/inputFiles/", "/outputFiles/");
                int lastSlashIndex = targetLocationInS3.lastIndexOf("/");
                String hashKey = targetLocationInS3.substring(0, lastSlashIndex);
                locationToCountTarget.put(hashKey, countLines);
                System.out.println("local_app put: " + hashKey + "--> " + countLines);
                locationToCurrentCounter.put(hashKey, 0);
                // Process S3 file URLs when a new task is received
                threadPool.submit(() -> processAndDivideS3File(body[0], workFileRatio, countLines));
                // Delete message from queue after processing
                aws.deleteMessage(localAppsQueueUrl, message.receiptHandle());
                if (needsToTerminate) {
                    // Retreives the number of total localApps that were sent until terminate
                    // message
                    totalLocalApps = locationToCountTarget.size();
                    System.out.println("Submitted last file & Termination message received.");
                    break;
                }
            }
        }
        System.out.println("listener of LocalApps finished his job");
    }

    protected void processAndDivideS3File(String inputFileLocationInS3, int workerFileRatio, int countLines) {
        System.out.println(Thread.currentThread() + ": is getting the necessary file");
        String[] splitLocation = inputFileLocationInS3.split("/");
        // Download file from S3
        File s3InputFile = new File(splitLocation[0] + "_" + splitLocation[2]); // localApp+ID_inputFileName
        aws.downloadFileFromS3(inputFileLocationInS3, s3InputFile);
        int indexOfCurrentPDF = 0;

        // Extract the base name
        String inputFileNameWithExt = splitLocation[2]; // input-sample-1.txt
        int dotIndex = inputFileNameWithExt.lastIndexOf(".");
        String inputFileNameWithoutExt = inputFileNameWithExt.substring(0, dotIndex); // input-sample-1 (without
        // extension)

        // Cut the targetLocationInS3 to be without the name of the input-file
        int lastSlashIndex = inputFileLocationInS3.lastIndexOf("/");
        String targetLocationInS3 = inputFileLocationInS3.substring(0, lastSlashIndex + 1).replace("/inputFiles/",
                "/outputFiles/");

        // Activating workers, according to number of files and workerFile ratio
        activateWorkers(countLines, workerFileRatio);

        // Send a message for each subFile in the input-file
        System.out.println(Thread.currentThread() + ": Starting the dividing process ");
        try (BufferedReader reader = new BufferedReader(new FileReader(s3InputFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // line=(operation originalUrl) targetLocation inputFileName_index (seperated by tabs)
                String[] lineSplit = line.split("\t");
                String operation = lineSplit[0];
                String PDFUrl = lineSplit[1];
                 aws.sendSQSMessage(
                        operation + "\t" + PDFUrl + "\t" + targetLocationInS3 + "\t" + inputFileNameWithoutExt + "\t" + indexOfCurrentPDF,
                        manager2WorkersQueueUrl);
                indexOfCurrentPDF++;
                System.out.println("Manager sent message to manager2Workers queue: " + indexOfCurrentPDF);
            }
        } catch (IOException e) {
            System.out.println("error in opening file in buffer");
        }
        if (s3InputFile.exists()) {
            s3InputFile.delete();
        }
    }

    protected void activateWorkers(int numOfPdfs, int workerFileRatio) {
        System.out.println(Thread.currentThread() + ": activating workers");
        int numWorkersToAdd;
        synchronized (lockActiveWorkers) {
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
            RunInstancesResponse response = aws.runInstanceFromAmiWithScript(InstanceType.T2_NANO, 1, 1, "Worker");
            String newInstanceId = response.instances().get(0).instanceId();
            aws.tagInstanceAsWorker(newInstanceId);
        }
    }

    protected void terminateThreadPool() {
        threadPool.shutdown(); // Initiates an orderly shutdown
        System.out.println("Started shutdown of ThreadPool");
        try {

            if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                threadPool.shutdownNow(); // Force shutdown if tasks take too long
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted exception: shutting down ThreadPool");
            threadPool.shutdownNow(); // Handle interruption
            Thread.currentThread().interrupt();
        }
    }

    protected void listenForWorkersTasks() {
        int numberOfAppsSubmitted = 0;
        while (true) {
            // The worker sends location of the URL in S3
            System.out.println(Thread.currentThread() + ": attempting to get messages from workers");
            List<Message> messages = aws.getSQSMessagesList(workers2ManagerQueueUrl, 10, 5);
            for (int i = 0; i < messages.size() && !messages.isEmpty(); i++) {
                Message message = messages.get(i);
                String locationInS3 = message.body(); // newFilePathInS3(_index in the name)
                int lastSlashInd = locationInS3.lastIndexOf("/");
                String hashKey = locationInS3.substring(0, lastSlashInd);
                // the current amount of files already
                int lastCounter = locationToCurrentCounter.getOrDefault(hashKey, 0);
                int updatedCounter = locationToCurrentCounter.put(hashKey, lastCounter + 1) + 1;
                System.out.println("updated counter from: " + lastCounter + "to " + updatedCounter);
                System.out.println("target counter: " + locationToCountTarget.get(hashKey));
                System.out.println("hashKey: " + hashKey);
                if (updatedCounter == locationToCountTarget.get(hashKey)) {
                    // Extract the substring up to the last '/' --->
                    // LocalApp+ID/outputFiles/malawach.txt ---> LocalApp+ID/outputFiles/
                    int lastSlashIndex = locationInS3.lastIndexOf('/');
                    String outputFolderPath = locationInS3.substring(0, lastSlashIndex + 1);
                    threadPool.submit(() -> mergeToSummaryAndUploadToS3(outputFolderPath));
                    numberOfAppsSubmitted++;
                }
                aws.deleteMessage(workers2ManagerQueueUrl, message.receiptHandle());
            }

            if (numberOfAppsSubmitted == totalLocalApps) {
                // All files have been submitted to merge task --> meaning no new missions to
                // submit
                terminateThreadPool();
                System.out.println("Thread Pool finishes its last tasks and will be terminated");
                break;
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                System.err.println(Thread.currentThread() + ": Interrupted while waiting for threads to complete.");
                Thread.currentThread().interrupt(); // Preserve interrupt status
            }
        }
        System.out.println("listener for Workers finished his job");
    }

    protected void mergeToSummaryAndUploadToS3(String outputFolderPath) {
        System.out.println(Thread.currentThread() + ": is creating the summary file");
        List<String> filesToMerge = aws.getFilesInFolder(outputFolderPath);
        System.out.println(Thread.currentThread() + ": sorting");
        //Sorting the sub-files by their index in the file 


        try{
            Collections.sort(filesToMerge, (String a, String b) -> {
                int lastIndex1 = a.lastIndexOf("_");
                int lastDotIndex1 = a.lastIndexOf(".");
                int lastIndex2 = b.lastIndexOf("_");
                int lastDotIndex2 = b.lastIndexOf(".");
                Integer index1 = Integer.parseInt(a.substring(lastIndex1 + 1, lastDotIndex1));
                Integer index2 = Integer.parseInt(b.substring(lastIndex2 + 1, lastDotIndex2));
                return index1.compareTo(index2);
            });
            System.out.println(Thread.currentThread() + ": sorted");
        } catch (Exception e) {
            System.err.println(Thread.currentThread() + ": Error during sorting: " + e.getMessage());
            e.printStackTrace();
            return;
        }
        System.out.println(Thread.currentThread() + ": sorted");

        //Downloading input file and 
        File LocalAppInputFile = new File("LocalAppInputFile.txt");
        List<String> inputFileList = aws.getFilesInFolder(outputFolderPath.replace("/outputFiles/", "/inputFiles/"));
        aws.downloadFileFromS3(inputFileList.get(0), LocalAppInputFile);

        File summaryFile = new File("summary.txt");

        // Extract localAppId
        String path = outputFolderPath.substring(outputFolderPath.indexOf("LocalApp"));
        String localAppId = path.substring("LocalApp".length(), path.indexOf("/", "LocalApp".length()));

        System.out.println("Starting merge proccess in summary.txt");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(summaryFile));
                BufferedReader inputReader = new BufferedReader(new FileReader(LocalAppInputFile))) {

            String inputLine;

            for (String newFileUrl : filesToMerge) {
                inputLine = inputReader.readLine();

                if (inputLine == null) {
                    break;
                }

                int lastSlashIndex = newFileUrl.lastIndexOf("/");
                String fileNameWithIndex = newFileUrl.substring(lastSlashIndex + 1);
                boolean isError = fileNameWithIndex.contains("Error");

                if (isError) {
                    System.out.println(Thread.currentThread() + ": handling error inside the merge");
                    File errorFile = new File("errorFile.txt");
                    try {
                        aws.downloadFileFromS3(newFileUrl, errorFile);

                        StringBuilder errorDescription = new StringBuilder();
                        try (BufferedReader errorReader = new BufferedReader(new FileReader(errorFile))) {
                            String errorLine;
                            while ((errorLine = errorReader.readLine()) != null) {
                                errorDescription.append(errorLine).append(" ");
                            }
                        }

                        writer.write(inputLine + "\t" + errorDescription.toString().trim());
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        if (errorFile.exists()) {
                            errorFile.delete();
                        }
                    }
                } else {
                    System.out.println("else - url is fine");
                    writer.write(inputLine + "\t" + newFileUrl);
                }

                writer.newLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (LocalAppInputFile.exists()) {
                LocalAppInputFile.delete();
            }
            if (summaryFile.exists()) {
                try {
                    aws.uploadFileToS3(outputFolderPath + "summaryFile/summary.txt" , summaryFile);
                    summaryFile.delete();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

            aws.sendSQSMessage(outputFolderPath + "summaryFile/summary.txt", aws.getQueueUrl("manager2Local" + localAppId));
        }
    }

    public void deActivateEC2Nodes() {
        List<Instance> workers = aws.getAllInstancesWithLabel(AWS.Label.Worker);
        for (Instance workerInstance : workers) {
            if (workerInstance.state().name() == InstanceStateName.RUNNING) {
                aws.terminateInstance(workerInstance.instanceId());
                // terminate running workers
            }
        }
        System.out.println("Terminated all Workers");

        List<Instance> managers = aws.getAllInstancesWithLabel(AWS.Label.Manager);
        for (Instance managerInstance : managers) {
            if (managerInstance.state().name() == InstanceStateName.RUNNING) {
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
