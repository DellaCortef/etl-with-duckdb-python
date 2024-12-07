# Importing necessary libraries
import os
import time
import gdown
import duckdb
import pandas as pd
from pandas import DataFrame
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Load environment variables from a .env file
load_dotenv()

# Function to establish a DuckDB connection
def db_conn():
    """Connects to the DuckDB database; creates the database if it does not exist."""
    return duckdb.connect(database='duckdb.db', read_only=False)

# Function to initialize the table for tracking processed files
def initialize_table(conn):
    """Create the table if it does not exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS file_history (
            file_name VARCHAR,
            processing_time TIMESTAMP
        )
    """)

# Function to register a processed file in the database
def register_file(conn, file_name):
    """Registers a new file in the database with the current time."""
    conn.execute("""
        INSERT INTO file_history (file_name, processing_time)
        VALUES (?, ?)
    """, (file_name, datetime.now()))

# Function to fetch the list of already processed files
def processed_files(conn):
    """Returns a set with the names of all files already processed."""
    return set(row[0] for row in conn.execute("SELECT file_name FROM file_history").fetchall())

# Function to download files from Google Drive using gdown
def download_files_from_google_drive(folder_url, local_directory):
    """
    Downloads all files from a specified Google Drive folder into a local directory.
    """
    # Load environment variables
    folder_url = os.getenv("FOLDER_URL")
    local_directory = os.getenv("LOCAL_DIRECTORY", "default_directory")  # Default directory if not set

    # Ensure the local directory exists
    os.makedirs(local_directory, exist_ok=True)

    # Use gdown to download the folder
    gdown.download_folder(folder_url, output=local_directory, quiet=False)

# Function to list all files and identify their types (CSV, JSON, or Parquet)
def list_files_and_types(directory):
    """
    Lists files and identifies whether they are CSV, JSON, or Parquet.
    """
    files_and_types = []
    for file in os.listdir(directory):
        if file.endswith(".csv") or file.endswith(".json") or file.endswith(".parquet"):
            full_path = os.path.join(directory, file)
            type = file.split(".")[-1]
            files_and_types.append((full_path, type))
    return files_and_types

# Function to read a file based on its type
def read_file(file_path, type):
    """
    Reads the file according to its type and returns a DuckDB DataFrame.
    """
    if type == 'csv':
        return duckdb.read_csv(file_path)
    elif type == 'json':
        return pd.read_json(file_path)
    elif type == 'parquet':
        return pd.read_parquet(file_path)
    else:
        raise ValueError(f"Unsupported file type: {type}")

# Function to transform the DataFrame by adding a total sales column
def transform(df):
    """
    Transforms the DataFrame by adding a new column 'total_sales'
    calculated as 'quantity' * 'value'.
    """
    # Rename columns to match SQL query expectations
    df = df.rename(columns={"quantidade": "quantity", "valor": "value"})
    
    # Execute the SQL query to calculate total sales
    df_transformed = duckdb.sql("SELECT *, quantity * value AS total_sales FROM df").df()
    print(df_transformed)
    return df_transformed

# Function to save the transformed DataFrame into a PostgreSQL database
def save_in_postgres(df, table):
    """
    Save the DataFrame in a PostgreSQL database table.
    """
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL is not set or loaded.")

    # Create a connection to the PostgreSQL database
    engine = create_engine(DATABASE_URL)
    df.to_sql(table, con=engine, if_exists='append', index=False)

# Main pipeline function to process files
def pipeline():
    """
    Main ETL pipeline to process files:
    1. Downloads files from Google Drive.
    2. Reads and transforms unprocessed files.
    3. Saves transformed data to PostgreSQL.
    4. Logs processed files in DuckDB.
    """
    # Load configurations
    folder_url = os.getenv("FOLDER_URL")
    local_directory = os.getenv("LOCAL_DIRECTORY", "default_directory")

    # Establish a DuckDB connection and initialize the tracking table
    conn = db_conn()
    initialize_table(conn)
    
    # Get the list of processed files and all files in the directory
    processed = processed_files(conn)
    files_and_types = list_files_and_types(local_directory)

    logs = []
    for file_path, type in files_and_types:
        file_name = os.path.basename(file_path)
        if file_name not in processed:
            # Process new files
            df = read_file(file_path, type)
            df_transformed = transform(df)
            save_in_postgres(df_transformed, "calculated_sales")
            register_file(conn, file_name)
            print(f"File {file_name} processed and saved.")
            logs.append(f"File {file_name} processed and saved.")
        else:
            # Skip already processed files
            print(f"File {file_name} has already been processed previously.")
            logs.append(f"File {file_name} has already been processed previously.")

    return logs

# Entry point of the script
if __name__ == "__main__":
    pipeline()