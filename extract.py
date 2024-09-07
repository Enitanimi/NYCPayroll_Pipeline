

import os
import dask.dataframe as dd
import logging
from datetime import datetime




# Set up logging
logging.basicConfig(filename='etl_extract.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')




def extract_latest_file(data_dir, file_pattern="nycpayroll_*.csv"):
    # Identify the latest file based on naming pattern
    files = [f for f in os.listdir(data_dir) if f.startswith(file_pattern.split('*')[0])]
    if not files:
        raise FileNotFoundError("No files matching the pattern found.")
    
    latest_file = max(files, key=lambda f: os.path.getctime(os.path.join(data_dir, f)))
    logging.info(f"Extracting data from {latest_file}")
    
    file_path = os.path.join(data_dir, latest_file)
    
    # Read the file with Dask
    df = dd.read_csv(file_path)
    
    return df, latest_file



def move_processed_file(src_dir, dest_dir, file_name):
    src_file_path = os.path.join(src_dir, file_name)
    dest_file_path = os.path.join(dest_dir, file_name)
    
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    
    os.rename(src_file_path, dest_file_path)
    logging.info(f"Moved {file_name} to {dest_dir}")



# Main extraction function
def extract_data(data_dir, archive_dir):
    try:
        df, latest_file = extract_latest_file(data_dir)
        logging.info("Data extraction successful")
        
        # After extraction, move file to archive
        move_processed_file(data_dir, archive_dir, latest_file)
        logging.info("File moved to archive")
        
        return df
    except Exception as e:
        logging.error(f"Error during extraction: {e}")
        raise
