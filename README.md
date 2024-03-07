# Data Extraction, Transformation, and Storage with Airflow and S3

This application fetches real estate listings from a webpage, tidies up the data, and stores it in an Amazon S3 bucket. 

It uses Python libraries like requests and BeautifulSoup for web scraping, and Boto3 for interacting with S3. 

Additionally, it's orchestrated by Apache Airflow, which schedules and manages the entire ETL process.

This setup automates the extraction, transformation, and loading of real estate data, making it easy to keep the listings up-to-date and accessible.

## Summary of the ETL Process Application:

### Data Retrieval:
- Utilizes requests library to send HTTP requests and BeautifulSoup library to parse HTML content of a webpage.
- Scrapes specific data from the webpage and saves it to a pandas dataframe.

### Data Preprocessing and Cleaning:
- Defines functions to extract specific details from the dataframe and clean the data.
- Extracts details like area, number of bedrooms, suites, parking spaces, etc.
- Cleans and preprocesses price data.

### Loading Data to Amazon S3:
- Loads AWS configuration from a JSON file.
- Uploads a CSV string to an S3 bucket using Boto3 S3 client.

### DAG and Task Dependencies:
- Sets up a Directed Acyclic Graph (DAG) named "automated_etl_process" using Airflow.
- DAG is scheduled to run daily.
- Defines three tasks for data extraction, transformation, and loading, represented by PythonOperator.
- Establishes task dependencies where the extraction task precedes transformation, and transformation precedes loading.

