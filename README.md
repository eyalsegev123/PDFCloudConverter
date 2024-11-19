# **PDF Cloud Converter**  

**PDF Cloud Converter** is a distributed cloud-based system designed to convert PDF documents into various formats efficiently and at scale. The project leverages AWS services like S3 (Simple Storage Service) for file storage, EC2 (Elastic Compute Cloud) instances for processing tasks, and SQS (Simple Queue Service) for communication between system components.  

## **Features**
- **Distributed Architecture**: Utilizes EC2 instances to distribute tasks between Manager and Worker nodes.  
- **Cloud Storage**: Upload, retrieve, and manage files seamlessly using S3.  
- **Task Management**: Implements SQS to coordinate the flow of tasks between components.  
- **Scalable Processing**: Dynamically launches or manages instances based on the workload.  
- **Robust File Handling**: Includes utility classes for efficient handling of PDF files, supporting multiple use cases.

## **Components**
1. **LocalApp**:  
   - The entry point for users.  
   - Reads input files, uploads them to S3, and communicates with the Manager node through SQS.  

2. **Manager**:  
   - Acts as a central controller.  
   - Receives tasks from LocalApp via SQS, delegates them to Worker nodes, and reports back upon completion.  

3. **Worker**:  
   - Processes individual tasks as assigned by the Manager.  
   - Retrieves input files from S3, performs conversions, and stores the results back to S3.  

4. **AWS Utility**:  
   - Provides reusable functions for interacting with AWS services, including S3, SQS, and EC2.

## **Technology Stack**
- **Language**: Java  
- **Cloud Services**: AWS S3, SQS, EC2  
- **Build Tool**: Maven  

## **How to Use**
1. Clone the repository:  
   ```bash
   git clone https://github.com/yourusername/PDFCloudConverter.git
