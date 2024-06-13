# Pinterest Data Pipeline

This project focuses on creating a robust data pipeline for extracting, processing, and analyzing data sourced from Pinterest.

## Overview

The Pinterest Data Pipeline project is designed to achieve the following objectives:

- Extract data from Pinterest using the Pinterest API.
- Process and transform the extracted data into a structured format suitable for analysis.
- Load the processed data into a data storage system for further analysis and visualization.

## Getting Started

To begin using this project, follow these steps:

1. **Clone the repository**: 
   ```bash
   git clone https://github.com/JenHolmes608/pinterest-data-pipeline.git

2. Set up your environment:

Install the necessary dependencies using pip or conda.
Configure your Pinterest API credentials and any other required configurations.

3. Run the data pipeline:

Execute the Python scripts or notebooks provided to extract, process, and load the data.
Monitor the pipeline execution and address any errors or issues encountered.

##Contents
###0afff69adbe3_dag.py
Defines a Directed Acyclic Graph (DAG) named 0afff69adbe3_dag for scheduling tasks related to data processing and analysis. Tasks include submitting Databricks notebook tasks to an existing cluster.

###pinterest_batch_data
This directory contains notebooks and scripts focused on batch processing and analysis of Pinterest data:

###pinterest_kinesis_data
Utilizes the Databricks environment for Spark-based data processing and streaming:
Reads data from respective Kinesis streams.
Converts binary data to JSON format.
Parses JSON into DataFrames using predefined schemas.
Applies custom cleaning functions (clean_df_pin, clean_df_geo, clean_df_user) to prepare data.
Visualizes cleaned dataframes.
Writes cleaned stream data to Delta tables in append mode.

###user_posting_emulation.py
Enables continuous data sending from an AWS RDS MySQL database to Kafka topics:
Multiprocessing: Facilitates concurrent data fetching and sending.
Integration with AWS Services: Utilizes API Gateway for invoking Kafka topic APIs.

###user_posting_emulation_streaming.py
Facilitates continuous data extraction from an AWS RDS MySQL database and posts it to AWS Kinesis streams:
Data Extraction: Fetches data using multiprocessing for efficiency.
AWS Integration: Posts data to Kinesis streams via API Gateway endpoints.


##Contributing
Contributions to this project are welcomed! If you have ideas for improvements or new features, feel free to open an issue or submit a pull request. Please review the Contribution Guidelines before contributing.

##License
This project is licensed under the MIT License.

For questions or feedback, please contact Jen Holmes.
