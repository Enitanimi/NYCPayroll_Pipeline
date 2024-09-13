# NYCPayroll_Pipeline

End to End pipeline orchestration

# PROJECT_NYCPAYROLL


## PROJECT OVERVIEW

The NYCPayroll project aims to design an automated ETL pipeline for processing payroll data for New York City. The pipeline is designed to handle data that is uploaded monthly into Azure Data Lake Storage (ADLS). It automates the data processing flow, ensuring the data is accurately and efficiently transferred from the source to the destination - performs extraction, transformation, and loading (ETL) into a Snowflake Data Warehouse. The pipeline is scalable, leveraging Dask to accommodate future data growth without significant refactoring. 



### **Technical Stack** 

Dask: For scalable and parallel processing. 

Pandas: For data manipulation. 

Azure Data Lake Storage (ADLS): For containerized data storage. 

Snowflake: For data warehousing. 

Task Scheduler: For orchestrating workflows. 



### **Project Structure**

NYC_Payroll_Pipeline/
│
├── data_extraction/
│   ├── extract.py                 # Script to extract data from source (ADLS)
│   └── archive/                   # Directory for storing processed files
│   └── Raw_Dataset/               # Directory for storing extracted files
│
├── data_transformation/
│   ├── transform.py               # Script to transform raw data
│   ├── dim_date.csv               # Date dimension table generated from raw data
│   ├── Cleaned_Dataset/           # Directory for transformed data files
│
├── data_loading/
│   ├── load.py                     # Script for loading transformed data into Snowflake (staging + EDW)
│
├── main_etl.py                     # The main ETL orchestration script 
│   ├── load.py                     # Script for loading transformed data into Snowflake (staging + EDW)
│
├── edw/
│   ├── staging_area/
│   │   ├── Dim_Employee.sql        # SQL for staging Dim_Employee table
│   │   ├── Dim_Agency.sql          # SQL for staging Dim_Agency table
│   │   ├── Dim_PayBasis.sql        # SQL for staging Dim_PayBasis table
│   │   ├── Dim_Date.sql            # SQL for staging Dim_Date table
│   │   ├── FactPayroll.sql         # SQL for staging FactPayroll table
│   │   └── truncate_staging.sql    # SQL to truncate staging tables before loading new data
│   │
│   └── edw_tables/
│       ├── Percentage_Overtime_Budget.sql          # SQL for aggregate table: Percentage of Overtime Budget
│       ├── Total_Overtime_Budget.sql               # SQL for aggregate table: Total Overtime Budget
│       ├── Percentage_Base_Salary_Budget.sql       # SQL for aggregate table: Percentage of Base Salary Budget
│       ├── Total_Base_Salary_Budget.sql            # SQL for aggregate table: Total Base Salary Budget
│       ├── Agency_Financial_Allocation.sql         # SQL for aggregate table: Agency Financial Allocation
│       ├── Overall_Resource_Allocation.sql         # SQL for aggregate table: Overall Resource Allocation
│       ├── Employee_Resource_Allocation.sql        # SQL for aggregate table: Employee Resource Allocation
│       └── Overtime_Summary.sql                    # SQL for aggregate table: Overtime Summary
│
│
├── monitoring/configuration
│   ├── config.env                  # Environment variables and all credentials for configurations
│   ├── logging_config              # Centralized logging configuration for monitoring ETL
│       └── pipeline_exec.log/      # Custom logging configuration
│
│
├── automation/
│   ├── task_scheduler.bat          # Batch file to schedule the pipeline using Windows Task Scheduler
│
│
└── README.md                       # Overview of the project, setup instructions, and usage notes



### **Project Tasks

1. Design a Data Warehouse for NYC. 

2. Develop a scalable and automated ETL pipeline to load payroll data into the NYC Data Warehouse. 

3. Create aggregate tables in the Data Warehouse for analyzing key business questions. 

4. Ensure data quality and consistency throughout the pipeline. 

5. Set up a public user with limited privileges for public access to the NYC Data Warehouse. 

6. Document all processes for reproducibility and maintain a cloud-hosted repository for collaboration. 



### Setup and Installation 

1. ***Clone the Repository: ***


git clone https://github.com/your-repo/your-project.git 
cd your-project 
 

2. ***Install Dependencies:*** 
Ensure you have Python 3.12 or later installed. Then, install the required Python packages: 

pip install -r requirements.txt 
 

3. ***Set Up Environment Variables:*** 

Create a .env file in the project root directory with the following content: 

sn_user=<your_snowflake_user> 
sn_pword=<your_snowflake_password> 
sn_Acct_Id=<your_snowflake_account_id> 
sn_DB=<your_snowflake_database> 
sn_schema=<your_snowflake_schema> 
sn_DWH=<your_snowflake_warehouse> 
 

4. ***Configure Snowflake:***

 Ensure you have Snowflake access and the necessary permissions to create and manage tables. 

5. ***Batch File Setup:*** 

Modify batch_file.bat to reflect your Python installation path and save it. 


### Usage Instructions 

1. Run the ETL Pipeline: Execute the batch file to start the ETL process: 

batch_file.bat 
 

2. Monitor Logs: Check the etl_pipeline.log file for detailed logs of the ETL process. 

###**Scripts**###

*extract.py:* Handles the extraction of data from ADLS. 

*transform.py:* Performs data transformations and saves the results. 

*load.py:* 

Connects to Snowflake and creates necessary tables. 

Loads data into Snowflake tables and handles logging.

*main.py:*  

Orchestrates the ETL process

Logs the start and completion of each step. 

Executes the stored procedure after data loading. 

*pipeline_log.py* - Error Handling Log Script

Configures logging for error tracking and debugging. 

*Batch File* - 

Executes the ETL pipeline and logs the output to a file. 

### Known Issues###

- a. Moving Extracted Data to Another Folder for Archiving: 

*Issue*: Difficulty in moving extracted data to an archive folder after processing. 

*Resolution*: This step can be handled in the ETL pipeline by adding a step to move the files after extraction and transformation. Ensure the pipeline script includes commands to move files to an archive folder once they are processed. 

- b. Setting the Dask DataFrame with Different Data Types: 

*Issue*: Dask didn’t accept different datatypes in the dataset without specifying each column and its datatype. 

*Resolution*: Explicitly define column datatypes when creating the Dask DataFrame to handle diverse datatypes properly. 

- c. Generating the Unique Identifier in the FactPayroll Table: 

*Issue*: Generating a unique identifier (FactID) in the FactPayroll table. 

*Resolution*: Added a column for FactID in the DataFrame and used a sequential range to generate unique IDs. 

- d. Executing the Stored Procedure in Snowflake from the Main Script: 

*Issue*: Integrating and executing the Snowflake stored procedure from the main script. 

*Resolution*: Implemented the execution of the stored procedure in the main script after loading data into Snowflake. 

 

Contributors 

Oluwatoyin 