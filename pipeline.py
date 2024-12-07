# Importing libs
import os
import gdown
import duckdb
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Load environment variables from a .env file
load_dotenv()

# Define function to download files from Google Drive
def download_files_from_google_drive(folder_url, local_directory):
    # Load environment variables
    folder_url = os.getenv("FOLDER_URL")
    local_directory = os.getenv("LOCAL_DIRECTORY", "default_directory")  # Default if not set

    # Ensure the directory exists
    os.makedirs(local_directory, exist_ok=True)

    # Use gdown to download the folder
    gdown.download_folder(folder_url, output=local_directory, quiet=False)

# Function to list CSV files in the specified directory
def files_list(directory):
    csv_files = []
    all_files = os.listdir(directory)
    for file in all_files:
        if file.endswith(".csv"):
            file_path = os.path.join(directory, file)
            csv_files.append(file_path)
    print(csv_files)
    
    return csv_files


if __name__ == "__main__":
    folder_url = os.getenv("FOLDER_URL")
    local_directory = os.getenv("LOCAL_DIRECTORY", "default_directory")
    # download_files_from_google_drive(folder_url, local_directory)
    files_list(local_directory)