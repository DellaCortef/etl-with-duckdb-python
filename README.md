# etl-with-duckdb-python

## Project - Part 1

- how to treat different files;
- how to validate data quality;
- how to automate ETL;
- we will use docker to deploy;


## Steps
**1.1 Choose Python version:**
- python 3.12.1

**1.2 Setting virtual environment with poetry**
- poetry init
- poetry shell

**1.3 Adding front-end**
- poetry add streamlit

**1.4 Creating "Hello World" file:**
- app.py

**1.5 Deploy from my local host**
- release for others to access
- we will choose Render by Business requirement -> no cost
- creating docker file and setting the configs


## Project - Part 2

- we will use Google Drive as a central data repository;
- how to deal with parquet, json and csv files;
- use python and duckdb to transform data;
- how to save data in a sql database;

## ETL Pipeline Flow

Below is the flow diagram of the ETL pipeline process implemented in the code:

```mermaid
graph TD
    A[Start] --> B[Load Environment Variables]
    B --> C[Establish DuckDB Connection]
    C --> D[Initialize File History Table]
    D --> E[Fetch Processed Files]
    E --> F{Check Files in Directory}
    F --> G[List Files and Types]
    G --> H{File Already Processed?}
    H -- Yes --> I[Skip File]
    H -- No --> J[Read File Based on Type]
    J --> K[Transform Data]
    K --> L[Save Transformed Data to PostgreSQL]
    L --> M[Register File in DuckDB]
    M --> F
    I --> F
    F --> N[End]

## Steps
**2.1 Choose Python version**
- Ensure you are using Python 3.12.1 for compatibility with the codebase and libraries.

```bash
pyenv install 3.12.1
pyenv local 3.12.1
python --version  # Verify Python version
```

**2.2 Setting virtual environment with poetry**
- Initialize and activate the virtual environment using Poetry.
```bash
poetry init  # Initialize Poetry for dependency management
poetry shell  # Activate the virtual environment
```

**2.3 Installing libs through poetry:**
- Install the required libraries for the project, each serving a specific purpose:
```bash
poetry add streamlit
# To create a dashboard interface (front-end).

poetry add gdown
# To download files directly from Google Drive.

poetry add duckdb
# To transform and process data with an in-memory database.

poetry add psycopg2-binary
# To connect and interact with PostgreSQL databases.

poetry add python-dotenv
# To securely load environment variables from a `.env` file.

poetry add psycopg
# To enhance PostgreSQL operations.

poetry add sqlalchemy
# To handle database operations and ORM capabilities.
```

**2.4 Downloading Google Drive files**
- Use gdown to download files from a shared Google Drive folder into a local directory.
```python
# Example of downloading files with gdown
download_files_from_google_drive(FOLDER_URL, local_directory="data")
```

**2.5 Listing files in a directory**
- A function to list files and identify their formats in a given directory.
```python
files_and_types = list_files_and_types("data")
print(files_and_types)
# Output:
# [('data/sales_05_01_2024.csv', 'csv'), ('data/sales_06_01_2024.json', 'json'), ('data/sales_07_01_2024.parquet', 'parquet')]
```

**2.6 Reading files using DuckDB**
- Read files based on their type and convert them into DuckDB or Pandas DataFrames.

**2.7 Checking file format**
- The code supports the following file formats:
    - CSV
    - JSON
    - Parquet

**2.8 Function to add a total sales column**
- Transform the data by adding a calculated total_sales column.

**2.9 Saving transformed data into PostgreSQL**
- Save the processed data into a PostgreSQL table using SQLAlchemy.

**2.10 Tracking processed files**
- A history of processed files is stored in DuckDB to avoid duplicate processing.

**2.11 Complete ETL Pipeline**
- The pipeline automates:
    - Downloading files
    - Identifying formats
    - Reading and transforming data
    - Saving transformed data into PostgreSQL
    - Logging processed files in DuckDB

## Settings
- python -> 3.12
- environment manager -> poetry
- tool.poetry.dependencies:
    - python = ">=3.9,<3.9.7 || >3.9.7,<4.0"
    - streamlit = "^1.40.2"
    - gdown = "^5.2.0"
    - duckdb = "^1.1.3"
    - psycopg2-binary = "^2.9.10"
    - python-dotenv = "^1.0.1"
    - psycopg = "^3.2.3"
    - sqlalchemy = "^2.0.36"