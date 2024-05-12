import os
import random
import logging
import pandas as pd
from pyspark.sql import SparkSession
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import great_expectations as gx
from great_expectations.checkpoint import Checkpoint
from great_expectations.data_context import BaseDataContext
from great_expectations.dataset import PandasDataset
from airflow.operators.python import PythonOperator

RAW_DATA_DIR = '/opt/airflow/raw_data'
GOOD_DATA_DIR = '/opt/airflow/good_data'
BAD_DATA_DIR = '/opt/airflow/bad_data'
PROJECT_CONFIG_PATH = '/opt/airflow/gx/great_expectations.yml'
PROJECT_CONFIG_DIR = '/opt/airflow/gx'

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
                context_root_dir = "/opt/airflow/great_expectations"
                context = gx.get_context(context_root_dir=context_root_dir)
                df = pd.read_csv(file_path)

                spark = SparkSession.builder.getOrCreate()
                dataframe_datasource = context.sources.add_or_update_spark(
                    name="my_spark_in_memory_datasource",
                )

                csv_file_path = file_path
                df = spark.read.csv(csv_file_path, header=True)
                dataframe_asset = dataframe_datasource.add_dataframe_asset(
                    name="data_chunk",
                    dataframe=df,
                )

                batch_request = dataframe_asset.build_batch_request()

                expectation_suite_name = "expect_column_values_to_not_be_null"
                context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)
                
                validator = context.get_validator(
                    batch_request=batch_request,
                    expectation_suite_name=expectation_suite_name,
                )
                validator.expect_column_values_to_not_be_null(column="quality")

                my_checkpoint_name = "my_databricks_checkpoint"

                checkpoint = Checkpoint(
                    name=my_checkpoint_name,
                    run_name_template="%Y%m%d-%H%M%S-my-run-name-template",
                    data_context=context,
                    batch_request=batch_request,
                    expectation_suite_name=expectation_suite_name,
                    action_list=[
                        {
                            "name": "store_validation_result",
                            "action": {"class_name": "StoreValidationResultAction"},
                        },
                        {"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}},
                    ],
                )
                context.add_or_update_checkpoint(checkpoint=checkpoint)
                checkpoint_result = checkpoint.run()

                results = checkpoint_result

                if results["success"]:
                    move_file(file_path, GOOD_DATA_DIR)
                    logging.info("No data quality issues found. File moved to good_data directory.")
                    return {"validation_results": results, "good_data": df}  # Return a dictionary with the validation results and the good data
                else:
                    logging.warning("Data quality issues found.")
                    for result in results['results']:
                        logging.warning(f"Error: {result['expectation_config']['kwargs']['column']} - {result['result']}")

                    bad_data = df.loc[~df.index.isin(results['successful_rows'])]  # Get the rows with data errors

                    move_file(file_path, BAD_DATA_DIR)  # Move the file to the bad_data directory

                    logging.info("File split into good_data and bad_data directories.")
                    return {"validation_results": results, "good_data": df, "bad_data": bad_data}  # Return a dictionary with the validation results, the good data, and the bad data
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
        logging.info(f"Validation results: {validation_results['validation_results']}")

        if 'bad_data' in validation_results:
            bad_data = validation_results['bad_data']
            save_split_data(file_path, bad_data, BAD_DATA_DIR)  # Save the bad data to a file in the bad_data directory
            logging.info("Bad data saved to bad_data directory.")
        else:
            good_data = validation_results['good_data']
            save_split_data(file_path, good_data, GOOD_DATA_DIR)  # Save the good data to a file in the good_data directory
            logging.info("Data saved to good_data directory.")

    @task
    def save_split_data(original_file: str, data: pd.DataFrame, destination_dir: str) -> None:
        file_name = os.path.basename(original_file)
        split_file_name = f"split_{file_name}"
        dest_path = os.path.join(destination_dir, split_file_name)
        data.to_csv(dest_path, index=False)

    @task
    def send_alerts(file_path: str, validation_results: dict) -> None:
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
    send_alerts_task = send_alerts(read_task, validate_and_split_task)
    save_data_errors_task = save_data_errors(validate_and_split_task)

ingest_wine_data_dag = ingest_wine_data()