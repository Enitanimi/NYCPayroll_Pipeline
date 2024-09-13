

from datetime import datetime
from azure.storage.blob import BlobServiceClient
import os
import logging
from pipeline_log import logging
from dotenv import load_dotenv

load_dotenv(override=True)

account_key = os.getenv('AzureAK')
connection_string = os.getenv('AzureCS')
account_name = os.getenv('AzureAN')
container_name = os.getenv('AzureCN')



# Initialize BlobServiceClient

def init_blob_service_client(connection_string):

    """Initialize the blob service client."""

    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        return blob_service_client
    
    except Exception as e:
        logging.error(f"Error initializing BlobServiceClient: {e}")
        raise


# List blobs in the container and find the latest file based on the name or last modified time

def get_latest_blob(container_client, folder_path):

    """Retrieve the latest blob (file) from the specified ADLS folder."""

    try:
        blobs = container_client.list_blobs(name_starts_with=folder_path)
        latest_blob = None

        for blob in blobs:

            # Check if it's the first blob or newer than the current latest

            if not latest_blob or blob.last_modified > latest_blob.last_modified:
                latest_blob = blob

        return latest_blob.name if latest_blob else None
    
    except Exception as e:
        logging.error(f"Error retrieving the latest blob: {e}")
        raise


# Download the latest file from ADLS

def download_latest_file(blob_service_client, container_name, folder_path, local_download_path):

    """Download the latest file from ADLS and save it to the local directory."""

    try:
        container_client = blob_service_client.get_container_client(container_name)
        latest_blob_name = get_latest_blob(container_client, folder_path)

        if latest_blob_name:

            # Define local file path
            local_file_path = os.path.join(local_download_path, os.path.basename(latest_blob_name))


            # Ensure the local directory exists - # Create the directory if it doesn't exist
            #if not os.path.exists(local_download_path):
             #   os.makedirs(local_download_path)


            # Download the blob
            blob_client = container_client.get_blob_client(latest_blob_name)
            with open(local_file_path, "wb") as f:
                f.write(blob_client.download_blob().readall())
            
            logging.info(f"Successfully downloaded the latest file: {latest_blob_name} to {local_file_path}")
            return local_file_path, latest_blob_name
        
        else:
            logging.info(f"No new files found in {folder_path}")
            return None, None
        
    except Exception as e:
        logging.error(f"Error downloading the latest file from ADLS: {e}")
        raise

# Archive the processed file in ADLS

'''
def archive_processed_file(blob_service_client, container_name, blob_name, archive_folder):

    """Move the processed file to the archive folder in ADLS."""

    try:
        container_client = blob_service_client.get_container_client(container_name)

        # Copy blob to archive folder
        source_blob = f"https://{blob_service_client.account_name}.blob.core.windows.net/{container_name}/{blob_name}"
        archive_blob_name = f"{archive_folder}/{os.path.basename(blob_name)}"
        container_client.get_blob_client(archive_blob_name).start_copy_from_url(source_blob)

        # Delete the original blob
        container_client.get_blob_client(blob_name).delete_blob()

        logging.info(f"Archived and deleted the original file: {blob_name}")

    except Exception as e:
        logging.error(f"Error archiving the file {blob_name}: {e}")
        raise
'''


def archive_processed_file(blob_service_client, container_name, blob_name, archive_folder):
    """Move the processed file to the archive folder in ADLS."""
    try:
        container_client = blob_service_client.get_container_client(container_name)

        # Copy blob to archive folder
        source_blob = f"https://{blob_service_client.account_name}.blob.core.windows.net/{container_name}/{blob_name}"
        archive_blob_name = f"{archive_folder}/{os.path.basename(blob_name)}"
        
        # Log the URLs and paths for debugging
        logging.info(f"Source blob URL: {source_blob}")
        logging.info(f"Archive blob path: {archive_blob_name}")
        
        # Copy the blob
        container_client.get_blob_client(archive_blob_name).start_copy_from_url(source_blob)

        # Delete the original blob
        container_client.get_blob_client(blob_name).delete_blob()

        logging.info(f"Archived and deleted the original file: {blob_name}")

    except Exception as e:
        logging.error(f"Error archiving the file {blob_name}: {e}")
        raise


# Main extraction process

def extract_latest_file_from_adls():

    """Main function to extract the latest file from ADLS and archive the processed files."""

    try:
        connection_string = os.getenv('AzureCS')
        container_name = os.getenv('AzureCN')
        raw_data_folder = "Dataset/raw_payroll_data"  # Path to the folder in ADLS
        archive_folder = "Dataset/archived_payroll_data"            # Path to the archive folder in ADLS
        local_download_path = r"All_Dataset/Raw_Dataset"         # Local folder to save downloaded files


        # Initialize Blob Service Client
        blob_service_client = init_blob_service_client(connection_string)

        # Download the latest file from ADLS
        local_file_path, blob_name = download_latest_file(blob_service_client, container_name, raw_data_folder, local_download_path)


        if local_file_path and blob_name:

            # After successful processing, archive the file
            archive_processed_file(blob_service_client, container_name, blob_name, archive_folder)

            logging.info(f"File {blob_name} processed and archived successfully.")
        else:
            logging.info("No new files to process.")

    except Exception as e:
        logging.error(f"Error in the extraction process: {e}")
        raise

# Trigger the extraction process
if __name__ == "__main__":

    extract_latest_file_from_adls()
