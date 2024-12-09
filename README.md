# **PDF Cloud Converter**

## **Overview**  
PDF Cloud Converter is a distributed cloud-based system for converting PDF documents into different formats (TXT, IMG, or HTML). It works by running LocalApps to upload tasks to AWS. A Manager instance on AWS handles task distribution to Worker instances, which process the PDF files. Once the tasks are completed, the Manager merges the results and sends them back to the LocalApp, which generates an HTML file with links to the converted files.

## **Features**  
Distributed cloud-based system using AWS S3, SQS, and EC2.
Handles large numbers of files efficiently with parallel processing.
Fault-tolerant and scalable.
Supports dynamic resource allocation with a Manager-Worker architecture.
Easy to set up and run—just start the LocalApp, and the Manager and Workers will launch automatically.

## **Components**  

**LocalApp**  
   Reads the input file (with URLs and operations).
   Uploads the tasks to S3 and sends them to the Manager through SQS.
   Generates the final HTML file after processing.

**Manager**:
   Receives tasks from LocalApps via SQS.
   Launches and assigns tasks to Workers.
   Merges results and updates the LocalApps.

**Worker**:
   Processes individual tasks (e.g., converting a PDF to IMG/TXT/HTML).
   Uploads the processed files to S3 for the Manager.

**AWS Utility**:
   Manages S3 uploads/downloads, SQS messaging, and EC2 instance control.
   Technologies Used
   Programming Language: Java 17
   Build Tool: Maven
   Cloud Services: AWS S3, SQS, EC2
   

***Setup Instructions***

***Prerequisites***
Install:
   Java 17
   Maven
   AWS CLI

Configure AWS CLI:
   Run aws configure and set your credentials and default region.
   
   Build the project:
         mvn clean install
         Upload the jars to S3:

   manager.jar and worker.jar should be uploaded to S3 in the appropriate paths.
   
Run the LocalApp:

java -jar Local_App.jar <input_file> <output_file> <worker_ratio> <terminate?>
Example: java -jar Local_App.jar input-sample.txt summaryFile.html 4

Clean up resources manually:
   Use cleanup.jar to terminate EC2 instances, delete SQS queues, and clear S3 folders.

***AWS Setup Details***
AMI: Amazon Linux 2
Instance Type: t2.nano
Worker Limit: Max of 9 instances due to AWS account restrictions.

***Performance Metrics***
Tested on an input file with 2500 URLs.
Execution time: 

***System Design***
Security:
   No hardcoded AWS credentials; we used IAM roles.
   AWS CLI configuration is local and secure.
Scalability:
   We implemented two threads in the Manager:
   One listens for tasks from LocalApps.
   The other listens for task completions from Workers.
   Tasks are handled in a thread pool, allowing parallel execution while maintaining order.
Fault Tolerance:
   Tasks remain in the SQS queue until successfully completed.
   If a Worker fails, its task is reassigned to another Worker.
Termination Process:
   LocalApp sends a termination signal to the Manager.
   Manager ensures all tasks are completed before shutting down.
   Cleanup script (cleanup.jar) can be run manually to remove all AWS resources.
Threads – Pros and Cons:
   Advantages-
      1. Efficient parallelism: Tasks are processed faster using multiple threads.
      2. Scalability: Adding more threads/workers allows the system to handle larger workloads.
   Disadvantages- 
      Resource contention: Threads may compete for resources like CPU and memory.
      Latency: AWS service delays can affect task completion times.
Limitations:
   AWS restrictions limit us to 9 worker instances.
   Network latency between AWS services (SQS and S3) may slow down processing.

***Authors***
Tamir Nizri - 211621552
Eyal Segev - 315144717
Lior Hagay - 314872367