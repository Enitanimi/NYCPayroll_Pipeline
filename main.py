

from dotenv import load_dotenv
from extract import extract_latest_file_from_adls
from transform import process_new_dataset  # Import your transformation function
from load import load_data_to_snowflake, get_snowflake_engine  # Import the load function and Snowflake engine
from pipeline_log import logging  # Import logging module
import os
from sqlalchemy import text
import glob

load_dotenv(override=True)


def run_etl_pipeline():

    try:
       
        
        # Step 1: Extract the latest file from Azure Data Lake Storage (ADLS)

        logging.info("Starting extraction process...")
        extract_latest_file_from_adls()

         

        # Step 2: Transform the extracted data

        logging.info("Starting transformation process...")
       
        raw_dataset_directory = os.path.join("All_Dataset", "Raw_Dataset")
        process_new_dataset(raw_dataset_directory)  # Call the function to process the latest dataset
        

        # Step 3: Load transformed data into Snowflake

        logging.info("Starting load process...")

        transformed_dir = r"All_Dataset/Cleaned_Dataset/"
        dim_date_file_path = r"All_Dataset/dim_date/dim_date.csv"

        load_data_to_snowflake(transformed_dir, dim_date_file_path) 


        # Step 4: Execute the stored procedure in Snowflake to aggregate data

        logging.info("Executing stored procedure in Snowflake...")
        engine = get_snowflake_engine()
        with engine.connect() as conn:
            conn.execute(text('CALL "STG".PAYROLL_AGGREGATES();'))  # Update this procedure as per your Snowflake setup
            logging.info("Stored procedure executed successfully.")

        logging.info("ETL pipeline completed successfully.")


    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")
        raise


if __name__ == "__main__":
    run_etl_pipeline()

