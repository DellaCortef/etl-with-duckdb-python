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


## Steps
**2.1 Choose Python version:**
- python 3.12.1

**2.2 Setting virtual environment with poetry**
- poetry init
- poetry shell

**2.3 Installing libs through poetry:**
- poetry add streamlit:
    - to create dashboard (front-end)
- poetry add gdown:
    - library responsible for downloading Google Drive
- poetry add duckdb:
    - to transform the data
- poetry add psycopg2-binary:
    - to work with Postgre
- poetry add python-dotenv:
    - to use environment variables as security
- poetry add psycopg:
    - 
- poetry add sqlalchemy:
    - 

**2.4 Downloading Google Drive files**
- csv file
- json file
- parquet file

**2.5 Listing CSV files in a directory**
- sales_05_01_2024.csv
- sales_06_01_2024.csv

**2.6 Reading CSV file with duckdb**
- reading csv file

**2.7 Checking file format**
- csv format
- json format
- parquet format

**2.8 Function to add a total sales column**
- duckdb.read_file

**2.9 Function to loading table into Postgre DataBase**
- save_in_postgres()


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