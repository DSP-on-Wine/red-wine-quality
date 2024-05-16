import json
import os
import random
import logging
import shutil
import pandas as pd
import requests
from pyspark.sql import SparkSession
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import great_expectations as gx
from great_expectations.checkpoint import Checkpoint
import sqlalchemy as db
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

RAW_DATA_DIR = '/opt/airflow/raw_data'
GOOD_DATA_DIR = '/opt/airflow/good_data'
BAD_DATA_DIR = '/opt/airflow/bad_data'
TEMP_DATA_DIR = '/opt/airflow/temp_data'

# Define SQLAlchemy model
Base = declarative_base()

# Define SQLAlchemy model
Base = declarative_base()

class DataError(Base):
    __tablename__ = 'data_errors'

    id = db.Column(db.Integer, primary_key=True)
    file_name = db.Column(db.String)
    column_name = db.Column(db.String)
    expectation = db.Column(db.String)
    element_count = db.Column(db.Integer)
    unexpected_count = db.Column(db.Integer)
    unexpected_percent = db.Column(db.Float)
    unexpected_index_query = db.Column(db.String)
    unexpected_index_list = db.Column(db.ARRAY(db.String))
    timestamp = db.Column(db.TIMESTAMP)

class CorrectFormats(Base):
    __tablename__ = 'data_success'

    id = db.Column(db.Integer, primary_key=True)
    file_name = db.Column(db.String)
    expectation = db.Column(db.String)
    timestamp = db.Column(db.TIMESTAMP)

@dag(
    schedule_interval=timedelta(seconds=30),
    start_date=datetime(2024, 5, 16),
    catchup=False,
    tags=['data_ingestion'],
    concurrency=2,
)
def ingest_wine_data():
    ## move to temp dir, return new dir.
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
        if file_path:  ## TODO - not just checking filepath, check if filepath exists in in raw-data
            print(f"Validating file {file_path}...")
            try:
                context_root_dir = "/opt/airflow/great_expectations"
                context = gx.get_context(context_root_dir=context_root_dir)

                spark = SparkSession.builder.getOrCreate()
                dataframe_datasource = context.sources.add_or_update_spark(
                    name="my_spark_in_memory_datasource", ## TODO - change the name
                )

                current_file_path = file_path
                df = spark.read.csv(file_path, header=True)
                dataframe_asset = dataframe_datasource.add_dataframe_asset(
                    name="data_chunk",
                    dataframe=df,
                )

                batch_request = dataframe_asset.build_batch_request()

                row_and_column_suite = context.get_expectation_suite(expectation_suite_name='row_and_column_suite')
                out_of_range_and_duplicate_suite = context.get_expectation_suite(expectation_suite_name='null_out_of_range_and_duplicate_suite')


                row_and_column_checkpoint = Checkpoint(
                    name="row_and_column_checkpoint",
                    run_name_template="%Y%m%d-%H%M%S-row-and-column-validation",
                    data_context=context,
                    validator=context.get_validator(batch_request=batch_request, expectation_suite=row_and_column_suite),
                    action_list=[
                        {"name": "store_validation_result", "action": {"class_name": "StoreValidationResultAction"}},
                        {"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}}
                    ]
                )
                out_of_range_and_duplicate_checkpoint = Checkpoint(
                    name="null_out_of_range_and_duplicate_checkpoint",
                    run_name_template="%Y%m%d-%H%M%S-out-of-range-and-duplicate-validation",
                    data_context=context,
                    validator=context.get_validator(batch_request=batch_request, expectation_suite=out_of_range_and_duplicate_suite),
                    action_list=[
                        {"name": "store_validation_result", "action": {"class_name": "StoreValidationResultAction"}},
                        {"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}}
                    ]
                )
                context.add_or_update_checkpoint(checkpoint=row_and_column_checkpoint)
                context.add_or_update_checkpoint(checkpoint=out_of_range_and_duplicate_checkpoint)

            
                result_format: dict = {
                    "result_format": "COMPLETE",
                    "unexpected_index_column_names": ["index"],
                    "return_unexpected_index_query": True,
                }

                row_and_column_result = row_and_column_checkpoint.run(result_format=result_format)

                results = row_and_column_result
                print(results)

                validation_result = {
                    "to_split": 0,
                    "file_path": file_path,
                    "data_issues": [],
                    "timestamp": datetime.now(),
                    "correct_formats": []
                }
                
                if (not row_and_column_result["success"]):
                    print("File entirely invalid.")
                    move_file(file_path, BAD_DATA_DIR) 
                    returned_result = {}
                    for result in results['run_results'].values():
                        returned_result = result['validation_result']
                    for result in returned_result['results']:
                        print(f'Result was: {result['result']}')
                        if (result['expectation_config']['expectation_type'] == 'expect_column_to_exist'):
                            validation_result['data_issues'].append({
                                'column': result['expectation_config']['kwargs']['column'],
                                'expectation': result['expectation_config']['expectation_type'],
                            })
                        else: 
                            validation_result['data_issues'].append({
                                'expectation': result['expectation_config']['expectation_type'],
                                'result': result['result'],
                            })
                    return validation_result ## it has to break
                else: 
                    returned_result = {}
                    
                    for result in results['run_results'].values():
                        returned_result = result['validation_result']

                    for result in returned_result['results']:
                        validation_result['correct_formats'].append({
                            'expectation': result['expectation_config']['expectation_type']
                        })

                # Run the null, out of range and duplicate checkpoint
                out_of_range_and_duplicate_result = out_of_range_and_duplicate_checkpoint.run(result_format=result_format)
                results = out_of_range_and_duplicate_result

                if out_of_range_and_duplicate_result["success"]:
                    move_file(file_path, GOOD_DATA_DIR)
                    logging.info("No data quality issues found. File moved to good_data directory.")
                    returned_result = {}

                    for result in results['run_results'].values():
                        returned_result = result['validation_result']

                    for result in returned_result['results']:
                        validation_result['correct_formats'].append({
                            'expectation': result['expectation_config']['expectation_type']
                        })
                
                else:
                    returned_result = {}
                    for result in results['run_results'].values():
                        returned_result = result['validation_result']

                    for result in returned_result['results']:
                        if (not result['success']):
                            logging.info("Found the failure.")
                            logging.warning(f"Error: {result['expectation_config']['kwargs']['column']}")

                            # print(f"{result['result']}")

                            if (result['result']['unexpected_percent'] == 100.0):
                                logging.warning("Found all rows to be bad.")
                                move_file(file_path, BAD_DATA_DIR)   
                                validation_result['data_issues'].append({
                                    'column': result['expectation_config']['kwargs']['column'],
                                    'expectation': result['expectation_config']['expectation_type'],
                                    'result': result['result'],
                                })
                            else : 
                                logging.info("Some rows are good.")
                                validation_result['to_split'] = 1 
                                print(f'val to split is true, {validation_result['to_split']}.')
                                validation_result['data_issues'].append({
                                    'column': result['expectation_config']['kwargs']['column'],
                                    'expectation': result['expectation_config']['expectation_type'],
                                    'result': result['result'],
                                })
                        else: 
                            validation_result['correct_formats'].append({
                                'expectation': result['expectation_config']['expectation_type']
                            })

                logging.info(f"Found values with data issues, returned:\n{validation_result}.")

                return validation_result
                    
            except Exception as e:
                logging.error(f"Error occurred while validating file {file_path}: {e}")
        else:
            logging.info("No file to validate.")
            return
    
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
        if validation_task and validation_task['to_split'] == 1:
            bad_rows = []
            data_issues = validation_task['data_issues']
            for issue in data_issues:
                indices = issue['result']['partial_unexpected_index_list']
                print(f"Validation results: {issue['result']}")
                for i in indices:
                    bad_rows.append(int(i['index']))

            logging.info(f"Splitting and saving data for file: {file_path}")
            logging.warning(f"Bad index results: {bad_rows}")

            df = pd.read_csv(file_path)
            good_df = df.loc[~df['index'].isin(bad_rows)]
            bad_df = df.loc[df['index'].isin(bad_rows)]

            file_name = os.path.basename(file_path).split('.')[0]

            good_file = f'{file_name + "_good"}.csv'
            bad_file = f'{file_name + "_bad"}.csv'

            good_df.to_csv(f'{GOOD_DATA_DIR}/{good_file}')
            logging.info(f"File split {good_file} created in good_data.")
            bad_df.to_csv(f'{BAD_DATA_DIR}/{bad_file}')
            logging.info(f"File split {bad_file} created in bad_data.")

            if os.path.exists(file_path):
                os.remove(file_path)
                logging.info(f"The file {file_path} has been deleted.")

        else:
            print(f"No data issues found for file {file_path}")


    def extract_values(data):
        print ("Extracting validated data.")
        file_path = data.get('file_path')
        data_issues = data.get('data_issues', [])
        time_stamp = data.get('timestamp')
        correct_formats = data.get('correct_formats', [])

        data_errors = []
        data_success = []
        for issue in data_issues:
            indices = ""
            for index in issue['result']['unexpected_index_list']:
                indices += str(index)
            data_error_entry = {
                'file_name': file_path,
                'column_name': issue['column'],
                'expectation': issue['expectation'],
                'element_count': issue['result']['element_count'],
                'unexpected_count': issue['result']['unexpected_count'],
                'unexpected_percent': issue['result']['unexpected_percent'],
                'unexpected_index_query': issue['result']['unexpected_index_query'],
                'unexpected_index_list': indices,
                'timestamp': time_stamp
            }
            data_errors.append(data_error_entry)
        print(f"Found {len(data_errors)} data errors.")
        
        for success in correct_formats:
            correct_entry = {
                'file_name': file_path,
                'expectation': success['expectation'],
                'timestamp': time_stamp
            }
            data_success.append(correct_entry)
        print(f"Found {len(data_success)} data success.")

        return data_errors, data_success
        

    def insert_data_to_database(data_to_save, session, is_issue: bool):
        try:
            if is_issue:
                print("Inserting issue.")
                data_errors = []
                for entry in data_to_save:
                    error_entry = {
                        'file_name': entry['file_name'],
                        'column_name': entry['column_name'],
                        'expectation': entry['expectation'],
                        'element_count': entry['element_count'],
                        'unexpected_count': entry['unexpected_count'],
                        'unexpected_percent': entry['unexpected_percent'],
                        'unexpected_index_query': entry['unexpected_index_query'],
                        'unexpected_index_list': entry['unexpected_index_list'],
                        'timestamp': entry['timestamp']
                    }
                    data_error = DataError(**error_entry)
                    data_errors.append(data_error)

                session.bulk_save_objects(data_errors)
                session.commit()
                print(f"{len(data_errors)} values inserted successfully.")

            else:
                print("Inserting success.")
                correct_formats_list = []
                for entry in data_to_save:
                    correct_entry = {
                        'file_name': entry['file_name'],
                        'expectation': entry['expectation'],
                        'timestamp': entry['timestamp']
                    }
                    correct_format = CorrectFormats(**correct_entry)
                    correct_formats_list.append(correct_format)

                session.bulk_save_objects(correct_formats_list)
                session.commit()
                print(f"{len(correct_formats_list)} values inserted successfully.")

        except Exception as e:
            print("Error inserting values:", e)

    @task
    def send_alerts(file_path: str, validation_results: dict) -> None:
        # Generate a report of the data problems
        # Send an alert using Teams
        # Include criticality, summary of errors, and link to the report
        payload = {
          "@type": "MessageCard",
          "@context": "http://schema.org/extensions",
          "summary": "Summary",
          "sections": [{
            "activityTitle": "Activity Title",
            "activitySubtitle": "Activity Subtitle",
            "facts": [
              {
                "file_name": file_path,
                "results": validation_results
              }
            ],
            "text": "Text"
          }],
          "potentialAction": [{
            "@type": "OpenUri",
            "name": "Link name",
            "targets": [{
              "os": "default",
              "uri": "https://epitafr.webhook.office.com/webhookb2/ba2cf95d-f0a7-4e10-9b19-0ad9cd217951@3534b3d7-316c-4bc9-9ede-605c860f49d2/IncomingWebhook/3c9355bdf9b14a9da2be02ab0866a064/721cb538-78e6-4e41-9f05-013bbc2d426d"
            }]
          }]
        }
        headers = {"content-type": "application/json"}
        requests.post("<insert_webhook_url_here>", json=payload, headers=headers)

    @task
    def save_data_errors(validation_results: dict) -> None:
        if validation_results['data_issues']:

            try:
                engine = db.create_engine('postgresql://postgres:postgres@host.docker.internal:5432/wine_quality')
                Session = sessionmaker(bind=engine)
                data_errors, data_success = extract_values(validation_results)
                session = Session()
                if (data_errors):
                    insert_data_to_database(data_errors, session, is_issue=1)
                if (data_success): 
                    insert_data_to_database(data_success, session, is_issue=0)
            except Exception as e:
                print("Error:", e)

    read_task = read_data()
    validate_task = validate_data(read_task)
    split_and_save_task = split_and_save_data(read_task, validate_task)
    send_alerts_task = send_alerts(read_task, validate_task)
    save_data_errors_task = save_data_errors(validate_task)

ingest_wine_data_dag = ingest_wine_data()