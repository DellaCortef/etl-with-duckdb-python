# Importing libs
import os
import time
import gdown
import duckdb
import pandas as pd
from pandas import DataFrame
from datetime import datetime
from dotenv import load_dotenv
from duckdb import DuckDBPyRelation
from sqlalchemy import create_engine

# Load environment variables from a .env file
load_dotenv()


def db_conn():
    """Connects to the DuckDB database; creates the database if it does not exist."""
    return duckdb.connect(database='duckdb.db', read_only=False)


def initialize_table(conn):
    """Create the table if it does not exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS file_history (
            file_name VARCHAR,
            processing_time TIMESTAMP
        )
    """)


def register_file(conn, file_name):
    """Registers a new file in the database with the current time."""
    conn.execute("""
    INSERT INTO file_history (file_name, processing_time)
    VALUES (?, ?)
""", (file_name, datetime.datetime.now()))


def processed_files(conn):
    """Returns a set with the names of all files already processed."""
    return set(row[0] for row in conn.execute("SELECT file_name FROM file_history").fetchall())


# Define function to download files from Google Drive
def download_files_from_google_drive(folder_url, local_directory):
    # Load environment variables
    folder_url = os.getenv("FOLDER_URL")
    local_directory = os.getenv("LOCAL_DIRECTORY", "default_directory")  # Default if not set

    # Ensure the directory exists
    os.makedirs(local_directory, exist_ok=True)

    # Use gdown to download the folder
    gdown.download_folder(folder_url, output=local_directory, quiet=False)

def list_files_and_types(directory):
    """Lists files and identifies whether they are CSV, JSON or Parquet."""
    files_and_types = []
    for file in os.listdir(directory):
        if file.endswith(".csv") or file.endswith(".json") or file.endswith(".parquet"):
            full_path = os.path.join(directory, file)
            type = file.split(".")[-1]
            files_and_types.append((full_path, type))
    return files_and_types


def read_file(file_path, type):
    """Reads the file according to its type and returns a DuckDB DataFrame."""
    if type == 'csv':
        return duckdb.read_csv(file_path)
    elif type == 'json':
        return pd.read_json(file_path)
    elif type == 'parquet':
        return pd.read_parquet(file_path)
    else:
        raise ValueError(f"Unsupported file type: {type}")
    

# Function to add a total sales column
def transform(df):
    # Execute the SQL query that includes the new column, operating on the virtual table
    df_transformed = duckdb.sql("SELECT *, quantity * value AS total_sales FROM df").df()
    # Remove record from virtual table for cleaning
    print(df_transformed)
    return df_transformed

def save_in_postgres(df, table):
    """Save the DataFrame in PostgreSQL."""
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL is not set or loaded.")

    engine = create_engine(DATABASE_URL)
    df.to_sql(table, con=engine, if_exists='append', index=False)



def pipeline():
    folder_url = os.getenv("FOLDER_URL")
    local_directory = os.getenv("LOCAL_DIRECTORY", "default_directory")

    # download_folder_google_drive(url_folder, local_directory)
    conn = db_conn()
    initialize_table(conn)
    processed = processed_files(conn)
    files_and_types = list_files_and_types(local_directory)

    logs = []
    for file_path, type in files_and_types:
        file_name = os.path.basename(file_path)
        if file_name not in processed:
            df = read_file(file_path, type)
            df_transformed = transform(df)
            save_in_postgres(df_transformed, "calculated_sales")
            register_file(conn, file_name)
            print(f"File {file_name} processed and saved.")
            logs.append(f"File {file_name} processed and saved.")

        else:
            print(f"File {file_name} has already been processed previously.")
            logs.append(f"File {file_name} has already been processed previously.")

    return logs
    

if __name__ == "__main__":
    pipeline()