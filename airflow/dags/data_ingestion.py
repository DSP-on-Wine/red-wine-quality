import json
import os
import random
import logging
import shutil
import pandas as pd
from pyspark.sql import SparkSession
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import great_expectations as gx
from great_expectations.checkpoint import Checkpoint


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
    def validate_data(file_path: str):
        if file_path:
            logging.info(f"Validating file {file_path}...")
            try:
                context_root_dir = "/opt/airflow/great_expectations"
                context = gx.get_context(context_root_dir=context_root_dir)

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
                suite = context.get_expectation_suite(expectation_suite_name='wine_expectation_suite')
                
                validator = context.get_validator(
                    batch_request=batch_request,
                    expectation_suite = suite
                )

                my_checkpoint_name = "my_databricks_checkpoint"

                checkpoint = Checkpoint(
                    name=my_checkpoint_name,
                    run_name_template="%Y%m%d-%H%M%S-my-run-name-template",
                    data_context=context,
                    validator=validator,
                    action_list=[
                        {
                            "name": "store_validation_result",
                            "action": {"class_name": "StoreValidationResultAction"},
                        },
                        {"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}},
                    ],
                )
                context.add_or_update_checkpoint(checkpoint=checkpoint)
                result_format: dict = {
                    "result_format": "COMPLETE",
                    "unexpected_index_column_names": ["index"],
                    "return_unexpected_index_query": True,
                }
                checkpoint_result = checkpoint.run(result_format=result_format)

                results = checkpoint_result
                logging.info(results)

                validation_result = {
                    "file_path": file_path,
                    "data_issues": []
                }

                if results["success"]:
                    move_file(file_path, GOOD_DATA_DIR)
                    logging.info("No data quality issues found. File moved to good_data directory.")
                    
                    return validation_result
                else:
                    logging.warning("Data quality issues found.")
                    returned_result = {}

                    for result in results['run_results'].values():
                        returned_result = result['validation_result']
                    
                    for result in returned_result['results']:
                        logging.info(f'Expected count was: {result['result']}')
                        if result['result']['unexpected_count'] != 0:
                            logging.warning(f"Error: {result['expectation_config']['kwargs']['column']} - {result['result']}")
                        
                            # Check if all rows are bad
                            if (result['result']['unexpected_percent'] == 100.0):
                                logging.info("Found all rows bad.")
                                move_file(file_path, BAD_DATA_DIR)                        
                            else : 
                                # return to split
                                validation_result['data_issues'].append({
                                    'column': result['expectation_config']['kwargs']['column'],
                                    'expectation': result['expectation_config']['expectation_type'],
                                    'result': result['result']
                                })

                    logging.info(f"Found values with data issues, returned:\n{validation_result}.")

                    return validation_result
            except Exception as e:
                logging.error(f"Error occurred while validating file {file_path}: {e}")
        else:
            logging.info("No file to validate.")
            return # Return an empty dictionary if no validation results are available

    def move_file(file_path: str, target_dir: str) -> None:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            target_path = os.path.join(target_dir, file_name)
            shutil.move(file_path, target_path)
            logging.info(f"Moved file {file_name} to {target_dir}.")
        else:
            logging.info(f"File {file_path} does not exist.")

    @task
    def split_and_save_data(file_path: str, validation_task) -> None:
        if validation_task['data_issues']:
            bad_rows = []
            data_issues = validation_task['data_issues']
            for issue in data_issues:
                indices = issue['result']['partial_unexpected_index_list']
                logging.info(f"Validation results: {issue['result']}")
                for i in indices:
                    bad_rows.append(int(i['index']))

            logging.info(f"Splitting and saving data for file: {file_path}")
            logging.info(f"Bad index results: {bad_rows}")

            df = pd.read_csv(file_path)
            good_df = df.loc[~df['index'].isin(bad_rows)]
            bad_df = df.loc[df['index'].isin(bad_rows)]

            file_name = os.path.basename(file_path).split('.')[0]

            good_file = f'{file_name + "_good"}.csv'
            bad_file = f'{file_name + "_bad"}.csv'
            
            good_df.to_csv(f'good_data/{good_file}')
            logging.info(f"File split {good_file} created in good_data.")
            bad_df.to_csv(f'bad_data/{bad_file}')
            logging.info(f"File split {bad_file} created in bad_data.")

            if os.path.exists(file_path):
                os.remove(file_path)
                logging.info(f"The file {file_path} has been deleted.")

        else:
            logging.info(f"No data issues found for file {file_path}")

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
    validate_task = validate_data(read_task)
    split_and_save_task = split_and_save_data(read_task, validate_task)
    send_alerts_task = send_alerts(read_task, validate_task)
    save_data_errors_task = save_data_errors(validate_task)

ingest_wine_data_dag = ingest_wine_data()