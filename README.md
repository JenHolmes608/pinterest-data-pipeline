# pinterest-data-pipeline
This project is aimed at creating a data pipeline for extracting, processing, and analyzing data from Pinterest.

## Overview

The Pinterest Data Pipeline project aims to:

- Extract data from Pinterest using the Pinterest API.
- Process and transform the extracted data into a format suitable for analysis.
- Load the processed data into a data storage system for further analysis and visualization.

## Getting Started

To get started with this project, follow these steps:

1. **Clone the repository**: 
   ```bash
   git clone https://github.com/JenHolmes608/pinterest-data-pipeline.git

2. Set up your environment:
  Install the necessary dependencies using pip or conda.
  Configure your Pinterest API credentials and other necessary configurations.

3. Run the data pipeline:
  Execute the Python scripts or notebooks to extract, process, and load the data.
  Monitor the pipeline and check for any errors or issues during execution.

Contents:
0afff69adbe3_dag.py
    DAG Definition: Defines a DAG named 0afff69adbe3_dag that starts on June 9, 2024, and runs daily (schedule_interval='@daily').
    Tasks:
    submit_run: Uses DatabricksSubmitRunOperator to submit a Databricks notebook task (notebook_task) to an existing Databricks cluster (existing_cluster_id).

pinterest_batch_data
    
    
    Contributing
    Contributions to this project are welcome! If you have ideas for improvements or new features, feel free to open an issue or submit a pull request.

Before contributing, please review the Contribution Guidelines.

License
This project is licensed under the MIT License.

For questions or feedback, please contact Jen Holmes.
