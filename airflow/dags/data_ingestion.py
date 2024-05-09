import os
import random
import logging
import json
import pandas as pd
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import great_expectations as ge
from airflow.operators.python import PythonOperator
from great_expectations.data_context import BaseDataContext
from great_expectations.dataset import PandasDataset

RAW_DATA_DIR = '/opt/airflow/raw_data'
GOOD_DATA_DIR = '/opt/airflow/good_data'
BAD_DATA_DIR = '/opt/airflow/bad_data'

@dag(schedule_interval=timedelta(days=1), start_date=datetime(2024, 5, 9), catchup=False, tags=['data_ingestion'])
def ingest_wine_data():

    @task
    def read_data() -> str:
        raw_files = os.listdir(RAW_DATA_DIR)

        if raw_files:
            random_file = random.choice(raw_files)
            file_path = os.path.join(RAW_DATA_DIR, random_file)
            logging.info(f"Selected file {random_file} from raw-data.")
            return file_path
        else:
            logging.info("No files found in raw-data directory.")
            return None

    @task
    def validate_and_split_data(file_path: str) -> dict:
        if file_path:
            logging.info(f"Validating file {file_path}...")
            try:
                # Read the CSV file using pandas
                df = pd.read_csv(file_path)
                # Convert the pandas DataFrame to a Great Expectations Dataset
                ge_dataset = PandasDataset(df)
                # Use Great Expectations to validate data
                context = BaseDataContext()  # Initialize a base data context
                suite = context.get_expectation_suite('expectation_suite.json')  # Load the expectation suite
                batch_kwargs = {'dataset': ge_dataset}  # Create the batch kwargs
                batch = context.get_batch(batch_kwargs, suite)  # Create the batch using the batch kwargs
                results = batch.validate()  # Validate the batch
                if results["success"]:
                    move_file(file_path, GOOD_DATA_DIR)
                    logging.info("No data quality issues found. File moved to good_data directory.")
                else:
                    logging.warning("Data quality issues found.")
                    for result in results['results']:
                        logging.warning(f"Error: {result['expectation_config']['kwargs']['column']} - {result['result']}")
                    validation_results = results  # Placeholder for the validation_results dictionary
                    return validation_results
            except Exception as e:
                logging.error(f"Error occurred while validating file {file_path}: {e}")
        else:
            logging.info("No file to validate.")
            return {}  # Return an empty dictionary if no validation results are available

    @task
    def move_file(file_path: str, destination_dir: str) -> None:
        if not os.path.exists(destination_dir):
            os.makedirs(destination_dir)
        file_name = os.path.basename(file_path)
        dest_path = os.path.join(destination_dir, file_name)
        os.rename(file_path, dest_path)

    @task
    def split_and_save_data(file_path: str, validation_results: dict) -> None:
        logging.info(f"Splitting and saving data for file: {file_path}")
        logging.info(f"Validation results: {validation_results}")
        # Split the file into good and bad data based on validation results
        good_data = []
        bad_data = []
        with open(file_path, 'r') as file:
            for line in file:
                # Check if line meets expectations
                if line_meets_expectations(line, validation_results):
                    good_data.append(line)
                else:
                    bad_data.append(line)
        # Save good and bad data to separate files
        save_split_data(file_path, good_data, GOOD_DATA_DIR)
        save_split_data(file_path, bad_data, BAD_DATA_DIR)

    def line_meets_expectations(line: str, validation_results: dict) -> bool:
        # Check if line meets all expectations
        for result in validation_results['results']:
            if not result['success']:
                column = result['expectation_config']['kwargs']['column']
                if column in line:
                    return False
        return True

    @task
    def save_split_data(original_file: str, data: list, destination_dir: str) -> None:
        file_name = os.path.basename(original_file)
        split_file_name = f"split_{file_name}"
        dest_path = os.path.join(destination_dir, split_file_name)
        with open(dest_path, 'w') as split_file:
            for line in data:
                split_file.write(line)

    @task
    def send_alerts(file_path: str) -> None:
        # Generate a report of the data problems
        # Send an alert using Teams
        # Include criticality, summary of errors, and link to the report
        pass

    @task
    def save_data_errors(validation_results: dict) -> None:
        # Save data problems along with data criticality to the database
        pass

    read_task = read_data()
    validate_and_split_task = validate_and_split_data(read_task)
    split_and_save_task = split_and_save_data(read_task, validate_and_split_task)
    send_alerts_task = send_alerts(read_task)
    save_data_errors_task = save_data_errors(validate_and_split_task)

ingest_wine_data_dag = ingest_wine_data()
    