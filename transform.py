

import dask.dataframe as dd
import pandas as pd
import logging
from pipeline_log import logging
from datetime import datetime
import glob  # Missing import for glob
import os  # Missing import for os

logging.info(f"Current working directory: {os.getcwd()}")


def generate_Dim_Date_Table(fiscal_years):

    """Generate a dim_date DataFrame based on fiscal years."""

    date_data = []
    for year in fiscal_years:
        DateID = int(f"{year}0000")  # Using 0000 for general fiscal year mapping
        date_data.append({
            "DateID": DateID,
            "Date": pd.Timestamp(f"{year}-01-01"),  # First day of the year as a placeholder
            "Year": year,
            "Month": 1,  # Placeholder value can be modified
            "Day": 1,    # Placeholder value can be modified
            "Quarter": 1 # Placeholder value can be modified
        })

    return pd.DataFrame(date_data)

# Automatically Process the Latest File in a Directory


def find_latest_file(directory):
    """
    Find the latest file in a given directory based on modification time.
    """

    logging.info(f"Scanning directory for files: {directory}")
    
    try:
        
        # searching for the latest file in the folder
        files = glob.glob(os.path.join(directory, "*.csv"))  # Only look for CSV files
        
        if not files:
            raise FileNotFoundError(f"No CSV files found in directory: {directory}")
        
        
        latest_file = max(files, key=os.path.getmtime)

        logging.info(f"Latest file found: {latest_file}")
        
        return latest_file

    except Exception as e:

        logging.error(f"Error finding latest file in directory: {e}")
        raise


def transform_data(input_file_path, output_folder, dim_date_output_folder):

    """
    Transform payroll data, including generating dim_date and saving transformed data.
    Function to transform raw data from the input folder and save the transformed data to the output folder.
    """

   # Check to see if input_path is a file
    if not os.path.isfile(input_file_path):

        raise FileNotFoundError(f"Input file does not exist: {input_file_path}")



    print(f"Transforming data from: {input_file_path}")

    # Define data types for columns

    dtypes = {
            "FiscalYear": "int64",
            "PayrollNumber": "int64",
            "AgencyID": "int64",
            "AgencyName": "object",
            "EmployeeID": "int64",
            "LastName": "object",
            "FirstName": "object",
            "AgencyStartDate": "object",
            "WorkLocationBorough": "object",
            "TitleCode": "int64",
            "TitleDescription": "object",
            "LeaveStatusasofJune30": "object",
            "BaseSalary": "float64",
            "PayBasis": "object",
            "RegularHours": "float64",
            "RegularGrossPaid": "float64",
            "OTHours": "float64",
            "TotalOTPaid": "float64",
            "TotalOtherPay": "float64"
        }

    logging.info("Starting data transformation")

    

    try:
        # Read the CSV file into a Dask DataFrame
        logging.info(f"Loading raw data from {input_file_path}")
        NYCpayroll_df = dd.read_csv(input_file_path, dtype=dtypes)
        
        # Drop duplicates
        NYCpayroll_df = NYCpayroll_df.drop_duplicates()


        # Convert date columns to datetime
        NYCpayroll_df["AgencyStartDate"] = dd.to_datetime(NYCpayroll_df["AgencyStartDate"], errors='coerce')

        # Rename columns
        NYCpayroll_df = NYCpayroll_df.rename(columns={"AgencyCode": "AgencyID"})


        # Create a unique identifier (PayBasisID) for each PayBasis
        paybasis_unique = NYCpayroll_df["PayBasis"].unique().compute()  # Extract unique PayBasis values
        paybasis_mapping = {pb: i + 1 for i, pb in enumerate(paybasis_unique)}  # Create a mapping dictionary
        NYCpayroll_df["PayBasisID"] = NYCpayroll_df["PayBasis"].map(paybasis_mapping, meta=("PayBasis", "int64"))


        # Fill missing values
        BaseSalary_mean = NYCpayroll_df["BaseSalary"].mean().compute()
        RegularHours_mean = NYCpayroll_df["RegularHours"].mean().compute()
        RegularGrossPaid_mean = NYCpayroll_df["RegularGrossPaid"].mean().compute()
        OTHours_mean = NYCpayroll_df["OTHours"].mean().compute()
        TotalOTPaid_mean = NYCpayroll_df["TotalOTPaid"].mean().compute()
        TotalOtherPay_mean = NYCpayroll_df["TotalOtherPay"].mean().compute()

        NYCpayroll_df = NYCpayroll_df.fillna({
            "FiscalYear": 0,
            "PayrollNumber": 0,
            "AgencyID": 0,
            "AgencyName": "Unknown",
            "EmployeeID": 0,
            "LastName": "Unknown",
            "FirstName": "Unknown",
            "AgencyStartDate": pd.Timestamp('1900-01-01'),  # Default date for missing values
            "WorkLocationBorough": "Unknown",
            "TitleCode": 0,
            "TitleDescription": "Unknown",
            "LeaveStatusasofJune30": "Unknown",
            "BaseSalary": BaseSalary_mean,
            "PayBasis": "Unknown",
            "RegularHours": RegularHours_mean,
            "RegularGrossPaid": RegularGrossPaid_mean,
            "OTHours": OTHours_mean,
            "TotalOTPaid": TotalOTPaid_mean,
            "TotalOtherPay": TotalOtherPay_mean,
            "PayBasisID": 0
        })


        # Convert Dask dataframe to Pandas to prepare for saving to CSV
        NYCpayroll_df = NYCpayroll_df.compute()

        # Output paths for cleaned data and dim_date

        cleaned_output_file = os.path.abspath(os.path.join(output_folder, "cleaned_dataset.csv"))
        dim_date_output_file = os.path.abspath(os.path.join(dim_date_output_folder, "dim_date.csv"))

        # Save the transformed data back to disk
        NYCpayroll_df.to_csv(cleaned_output_file, index=False)

        logging.info(f"Transformed data saved to {cleaned_output_file}")


        # Extract distinct fiscal years from the dataset
        fiscal_years = NYCpayroll_df["FiscalYear"].drop_duplicates().tolist()

        # Generate the dim_date DataFrame based on the fiscal years
        dim_date_df = generate_Dim_Date_Table(fiscal_years)

        # Save dim_date_df to CSV (for loading into Snowflake later)
        dim_date_df.to_csv(dim_date_output_file, index=False)

       
        logging.info("dim_date data generated and saved!")
   
    except Exception as e:
        
        logging.error(f"Error during data transformation: {e}")
        raise

    return NYCpayroll_df


# Dynamic file triggering:



def process_new_dataset(raw_data_directory):
    
    """
    Process the latest dataset from the raw data folder and transform it.
    """

    try:
        latest_file = find_latest_file(raw_data_directory)
        logging.info(f"Processing latest file: {latest_file}")

        # Folder paths for cleaned data and dim_date
        
        cleaned_data_folder = os.path.abspath(os.path.join('All_Dataset', 'Cleaned_Dataset'))
        dim_date_folder = os.path.abspath(os.path.join('All_Dataset', 'dim_date'))
        
        # Ensure the output folders exist
        
        os.makedirs(cleaned_data_folder, exist_ok=True)

        os.makedirs(dim_date_folder, exist_ok=True)

        # Call the transform function - # Pass the paths to the transform function

        transform_data(input_file_path=os.path.abspath(latest_file), output_folder=cleaned_data_folder, dim_date_output_folder=dim_date_folder)
    
    except Exception as e:

        logging.error(f"Error processing new dataset: {e}")
        raise



if __name__ == "__main__":


    
    raw_dataset_directory = os.path.abspath(os.path.join("All_Dataset", "Raw_Dataset"))

    process_new_dataset(raw_dataset_directory)