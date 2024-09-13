



import sqlalchemy
import snowflake.sqlalchemy
from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv
from pipeline_log import logging
from dask.distributed import Client
from sqlalchemy import text
import snowflake.connector
import dask.dataframe as dd

# Load environment variables

load_dotenv(override=True)


# Function to get the most recent file in a directory

def get_latest_csv_file(directory):

    csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {directory}")

    latest_file = max(csv_files, key=lambda f: os.path.getmtime(os.path.join(directory, f)))

    return os.path.join(directory, latest_file)



# Snowflake connection setup

def get_snowflake_engine():

    try:
        engine = create_engine(
            "snowflake://{user}:{password}@{account_identifier}/{database}/{schema}?warehouse={warehouse}".format(
                user=os.getenv("sn_user"),
                password=os.getenv("sn_pword"),
                account_identifier=os.getenv("sn_Acct_Id"),
                database=os.getenv("sn_DB"),
                schema=os.getenv("sn_schema"),
                warehouse=os.getenv("sn_DWH")
            )
        )
        logging.info("Connected to Snowflake successfully.")
        return engine
    except Exception as e:
        logging.error(f"Failed to connect to Snowflake: {e}")
        raise



# Function to load data to Snowflake

def load_data_to_snowflake(transformed_dir, dim_date_file_path):

    try:

        # Initialize Dask client with 1 worker

        client = Client(n_workers=1)
        logging.info("Dask client initialized with 1 worker.")
        
        # Get Snowflake engine

        engine = get_snowflake_engine()

        # Table definitions

        tables = [
            {
                "name": "Dim_Employee",
                "columns": [
                    {"name": "EmployeeID", "type": "INT"},
                    {"name": "LastName", "type": "VARCHAR(50)"},
                    {"name": "FirstName", "type": "VARCHAR(50)"},
                    {"name": "AgencyStartDate", "type": "DATE"},
                    {"name": "WorkLocationBorough", "type": "VARCHAR(255)"},
                    {"name": "TitleCode", "type": "INT"},
                    {"name": "TitleDescription", "type": "TEXT"},
                    {"name": "LeaveStatusasofJune30", "type": "VARCHAR(50)"}
                ]
            },
            {
                "name": "Dim_Agency",
                "columns": [
                    {"name": "AgencyID", "type": "INT"},
                    {"name": "AgencyName", "type": "VARCHAR(255)"}
                ]
            },
            {
                "name": "Dim_PayBasis",
                "columns": [
                    {"name": "PayBasisID", "type": "INT"},
                    {"name": "PayBasis", "type": "VARCHAR(255)"}
                ]
            },
            {
                "name": "Dim_Date",
                "columns": [
                    {"name": "DateID", "type": "INT"},
                    {"name": "Date", "type": "DATE"},
                    {"name": "Year", "type": "INT"},
                    {"name": "Month", "type": "INT"},
                    {"name": "Day", "type": "INT"},
                    {"name": "Quarter", "type": "INT"}
                ]
            },
            {
                "name": "FactPayroll",
                "columns": [
                    {"name": "FactID", "type": "INT"},
                    {"name": "EmployeeID", "type": "INT"},
                    {"name": "AgencyID", "type": "INT"},
                    {"name": "DateID", "type": "INT"},
                    {"name": "PayBasisID", "type": "INT"},
                    {"name": "FiscalYear", "type": "INT"},
                    {"name": "PayrollNumber", "type": "INT"},
                    {"name": "BaseSalary", "type": "FLOAT"},
                    {"name": "RegularHours", "type": "FLOAT"},
                    {"name": "OTHours", "type": "FLOAT"},
                    {"name": "RegularGrossPaid", "type": "FLOAT"},
                    {"name": "TotalOTPaid", "type": "FLOAT"},
                    {"name": "TotalOtherPay", "type": "FLOAT"}
                ]
            }
        ]


       
        # Get the latest file from transformed directory
        transformed_file_path = get_latest_csv_file(transformed_dir)
        logging.info(f"Transformed NYC Payroll CSV file '{transformed_file_path}' loaded successfully.")
        
         # Load the main transformed CSV file

        transformed_nycpayroll_df = dd.read_csv(transformed_file_path)
        
         # Log available columns
        logging.info(f"Columns in transformed DataFrame: {list(transformed_nycpayroll_df.columns)}")
        
         # Load dim_date 
         
        Dim_Date_df = dd.read_csv(dim_date_file_path)
        logging.info(f"Columns in Dim_Date_df: {Dim_Date_df.columns.tolist()}")



        # Establish Snowflake connection

        with engine.connect() as conn:


            # Create or truncate tables

            for table in tables:
                table_name = table["name"]
                columns = table["columns"]
                column_defs = ', '.join([f'"{col["name"]}" {col["type"]}' for col in columns])
                create_table_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({column_defs});'
                conn.execute(text(create_table_sql))

                logging.info(f'Table "{table_name}" created or already exists.')

                truncate_table_sql = f'TRUNCATE TABLE "{table_name}";'
                conn.execute(text(truncate_table_sql))

                logging.info(f'Truncated "{table_name}" successfully.')



            # Loading data into dimension and fact tables

            # Dim_Employee

            Dim_Employee_df = transformed_nycpayroll_df[["EmployeeID", "LastName", "FirstName", "AgencyStartDate",\
               "WorkLocationBorough",  "TitleCode", "TitleDescription", "LeaveStatusasofJune30"]]

           
            # Dim_Agency

            Dim_Agency_df = transformed_nycpayroll_df[["AgencyID", "AgencyName"]]

             
            # Dim_PayBasis

            Dim_PayBasis_df = transformed_nycpayroll_df[["PayBasisID", "PayBasis"]]

            # FactPayroll table

            factpayroll_df = transformed_nycpayroll_df.merge(Dim_Date_df, left_on="FiscalYear", right_on="Year", how="inner")\
            [["EmployeeID", "AgencyID", "DateID", "PayBasisID", "FiscalYear", "PayrollNumber", "BaseSalary",\
                "RegularHours", "OTHours", "RegularGrossPaid", "TotalOTPaid", "TotalOtherPay" ]]


            # Creating the FactID

            factpayroll_df["FactID"] = range(1, len(factpayroll_df) + 1)


            # Reorder columns to have FactID first

            factpayroll_df = factpayroll_df[["FactID", "EmployeeID", "AgencyID", "DateID", "PayBasisID",\
                "FiscalYear", "PayrollNumber", "BaseSalary", "RegularHours", "OTHours", "RegularGrossPaid",\
                 "TotalOTPaid", "TotalOtherPay" ]]


            # Converting Dask dataframe to pandas before loading to Snowflake

            Dim_Date_df = Dim_Date_df.compute()
            Dim_Date_df.to_sql("Dim_Date", con=conn, if_exists="replace", index=False)
            logging.info('Dim_Date data loaded successfully.')

            Dim_Employee_df = Dim_Employee_df.compute()
            Dim_Employee_df.to_sql("Dim_Employee", con=conn, if_exists="replace", index=False)
            logging.info('Dim_Employee data loaded successfully.')

            Dim_Agency_df = Dim_Agency_df.compute()
            Dim_Agency_df.to_sql("Dim_Agency", con=conn, if_exists="replace", index=False)
            logging.info('Dim_Agency data loaded successfully.')

            Dim_PayBasis_df = Dim_PayBasis_df.compute()
            Dim_PayBasis_df.to_sql("Dim_PayBasis", con=conn, if_exists="replace", index=False)
            logging.info('Dim_PayBasis data loaded successfully.')

            factpayroll_df = factpayroll_df.compute()
            factpayroll_df.to_sql("FactPayroll", con=conn, if_exists="replace", index=False)
            logging.info('FactPayroll data loaded successfully.')


            # Commit changes
            conn.commit()
            logging.info("All data loaded successfully.")


        # Close the Snowflake connection

        logging.info('Closing Snowflake connection.')
        conn.close()


        # Shut down the Dask client

        client.shutdown()
        logging.info("Dask client shutdown complete.")

    except Exception as e:
        logging.error(f"Error loading data to Snowflake: {e}")
        raise