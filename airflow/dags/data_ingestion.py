import os
import random
import logging
import shutil
import pandas as pd
import requests
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import great_expectations as gx
from great_expectations.checkpoint import Checkpoint
import sqlalchemy as db
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from airflow.exceptions import AirflowSkipException
from dotenv import load_dotenv

load_dotenv()

# Access the environment variables
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
VALIDATION_RESULT_URI = os.getenv('VALIDATION_RESULT_URI')
TEAMS_WEBHOOK = os.getenv('TEAMS_WEBHOOK')

RAW_DATA_DIR = os.getenv('RAW_DATA_DIR')
GOOD_DATA_DIR = os.getenv('GOOD_DATA_DIR')
BAD_DATA_DIR = os.getenv('BAD_DATA_DIR')
INGESTION_LOCK_FILE = os.getenv('INGESTION_LOCK_FILE')

# Define SQLAlchemy model
Base = declarative_base()


class DataError(Base):
    __tablename__ = 'data_errors'

    id = db.Column(db.Integer, primary_key=True)
    file_name = db.Column(db.String)
    column_name = db.Column(db.String)
    expectation = db.Column(db.String)
    unexpected_percent = db.Column(db.Float)
    unexpected_index_query = db.Column(db.String)
    observed_value = db.Column(db.String)
    criticality = db.Column(db.String)
    timestamp = db.Column(db.TIMESTAMP)


class CorrectFormats(Base):
    __tablename__ = 'data_success'

    id = db.Column(db.Integer, primary_key=True)
    file_name = db.Column(db.String)
    expectation = db.Column(db.String)
    timestamp = db.Column(db.TIMESTAMP)


@dag(
        schedule_interval=timedelta(seconds=90),
        start_date=datetime(2024, 5, 9),
        catchup=False,
        tags=['data_ingestion', 'dsp']
)
def ingest_wine_data():
    @task
    def read_data() -> str:
        raw_files = os.listdir(RAW_DATA_DIR)

        while raw_files:
            random_file = random.choice(raw_files)
            with open(INGESTION_LOCK_FILE, 'r') as f:
                if random_file in (s.rstrip() for s in f.readlines()):
                    continue
            with open(INGESTION_LOCK_FILE, 'a') as f:
                f.write(random_file)
                f.write('\n')

            file_path = os.path.join(RAW_DATA_DIR, random_file)

            logging.info(f"Selected file {random_file} from raw-data.")
            return file_path
        else:
            logging.info("No files found in raw-data directory.")
            raise AirflowSkipException
            return None

    @task
    def validate_data(file_path: str):
        if file_path:
            print(f"Validating file {file_path}...")
            try:
                context_root_dir = "/opt/airflow/great_expectations"
                context = gx.get_context(context_root_dir=context_root_dir)

                # spark = SparkSession.builder.getOrCreate()
                # dataframe_datasource = context.sources.add_or_update_spark(
                #     name="my_spark_in_memory_datasource", ## TODO - change the name
                # )

                # df = spark.read.csv(file_path, header=True)
                dataframe_datasource = context.sources.add_or_update_pandas(
                    name="my_pandas_data_validation",  
                )
                df = pd.read_csv(file_path)
                dataframe_asset = dataframe_datasource.add_dataframe_asset(
                    name="data_chunk",
                    dataframe=df,
                )

                batch_request = dataframe_asset.build_batch_request()
                row_and_column_suite = context.get_expectation_suite(
                    expectation_suite_name='row_and_column_suite'
                    )
                out_of_range_and_duplicate_suite = context.get_expectation_suite(
                    expectation_suite_name='null_out_of_range_suite'
                    )

                row_and_column_checkpoint = Checkpoint(
                    name="row_and_column_checkpoint",
                    run_name_template="%Y%m%d-%H%M%S-row-and-column-validation",
                    data_context=context,
                    validator=context.get_validator(
                        batch_request=batch_request, expectation_suite=row_and_column_suite
                        ),
                    action_list=[
                        {"name": "store_validation_result",
                         "action": {"class_name": "StoreValidationResultAction"}},
                        {"name": "update_data_docs",
                         "action": {"class_name": "UpdateDataDocsAction"}}
                    ]
                )
                out_of_range_and_duplicate_checkpoint = Checkpoint(
                    name="null_out_of_range_and_duplicate_checkpoint",
                    run_name_template="%Y%m%d-%H%M%S-out-of-range-and-duplicate-validation",
                    data_context=context,
                    validator=context.get_validator(
                        batch_request=batch_request,
                        expectation_suite=out_of_range_and_duplicate_suite
                        ),
                    action_list=[
                        {"name": "store_validation_result",
                         "action": {"class_name": "StoreValidationResultAction"}},
                        {"name": "update_data_docs",
                         "action": {"class_name": "UpdateDataDocsAction"}}
                    ]
                )
                context.add_or_update_checkpoint(
                    checkpoint=row_and_column_checkpoint
                    )
                context.add_or_update_checkpoint(
                    checkpoint=out_of_range_and_duplicate_checkpoint
                    )

                result_format: dict = {
                    "result_format": "COMPLETE",
                    "unexpected_index_column_names": ["index"],
                    "return_unexpected_index_query": True,
                }

                row_and_column_result = row_and_column_checkpoint.run(
                    result_format=result_format
                    )

                results = row_and_column_result
                print(results)

                validation_result = {
                    "to_split": 0,
                    "criticality": "",
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
                        validation_result['criticality'] = 'High'
                        if (result['expectation_config']['expectation_type'] == 'expect_column_to_exist'):
                            validation_result['data_issues'].append({
                                'column': result['expectation_config']['kwargs']['column'],
                                'expectation': result['expectation_config']['expectation_type'],
                                'unexpected_percent': '',
                                'unexpected_index_query': '',
                                'observed_value': ''
                            })
                        else:
                            validation_result['data_issues'].append({
                                'column': '',
                                'expectation': result['expectation_config']['expectation_type'],
                                'unexpected_percent': '',
                                'unexpected_index_query': '',
                                'observed_value': result['result']['observed_value'],
                            })
                    return validation_result  # it has to break
                else:
                    returned_result = {}

                    for result in results['run_results'].values():
                        returned_result = result['validation_result']

                    for result in returned_result['results']:
                        validation_result['correct_formats'].append({
                            'expectation': result['expectation_config']['expectation_type']
                        })

                # Run the null, out of range and duplicate checkpoint
                out_of_range_and_duplicate_result = out_of_range_and_duplicate_checkpoint.run(
                    result_format=result_format
                    )
                results = out_of_range_and_duplicate_result
                print(results)

                if out_of_range_and_duplicate_result["success"]:
                    move_file(file_path, GOOD_DATA_DIR)
                    logging.info(
                        "No data quality issues found. File moved to good_data directory."
                        )
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
                            logging.warning(
                                f"Error: {result['expectation_config']['kwargs']['column']}"
                                )

                            # print(f"{result['result']}")

                            if (result['result']['unexpected_percent'] == 100.0):
                                logging.warning("Found all rows to be bad.")
                                move_file(file_path, BAD_DATA_DIR)
                                indices = []
                                for index in result['result']['partial_unexpected_index_list']:
                                    indices.append(index['index'])
                                validation_result['data_issues'].append({
                                    'column': result['expectation_config']['kwargs']['column'],
                                    'expectation': result['expectation_config']['expectation_type'],
                                    'unexpected_percent': result['result']['unexpected_percent'],
                                    'unexpected_index_list': result['result']['unexpected_index_list'],
                                    'unexpected_index_query': result['result']['unexpected_index_query'],
                                    'observed_value': '',
                                })
                            
                            else:
                                logging.info("Some rows are good.")
                                validation_result['to_split'] = 1
                                print(f'val to split is true, {validation_result['to_split']}.')
                                if (result['result']['unexpected_percent'] >= 50.0):
                                    validation_result['criticality'] = 'High'
                                elif (result['result']['unexpected_percent'] >= 25.0):
                                    validation_result['criticality'] = 'Medium'
                                else:
                                    validation_result['criticality'] = 'Low'

                                indices = []
                                for index in result['result']['partial_unexpected_index_list']:
                                    indices.append(index['index'])
                                logging.info(f"Found indexes {indices} bad.")
                                validation_result['data_issues'].append({
                                    'column': result['expectation_config']['kwargs']['column'],
                                    'expectation': result['expectation_config']['expectation_type'],
                                    'unexpected_percent': result['result']['unexpected_percent'],
                                    'unexpected_index_list': result['result']['unexpected_index_list'],
                                    'unexpected_index_query': result['result']['unexpected_index_query'],
                                    'observed_value': '',
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
            raise AirflowSkipException

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
        logging.info(validate_task)
        if validation_task and validation_task['to_split'] == 1:
            logging.info(f"Complete issues: {validate_task['data_issues']}")
            bad_rows = []
            data_issues = validation_task['data_issues']
            for issue in data_issues:
                indices = issue.get('unexpected_index_list', [])
                logging.info(f"Validation results: {issue}")
                for i in indices:
                    bad_rows.append(int(i['index']))

            logging.info(f"Splitting and saving data for file: {file_path}")
            logging.warning(f"Bad index results: {bad_rows}")

            df = pd.read_csv(file_path)
            df.set_index('index', inplace=True)

            good_df = df.loc[~df.index.isin(bad_rows)]
            bad_df = df.loc[df.index.isin(bad_rows)]
            logging.info(f"Returned bad df as {bad_df}")
            logging.info(f"Returned good df as {good_df}")

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
            print(f"No unhandled data issues found for file {file_path}")
            raise AirflowSkipException

    def extract_values(data):
        print("Extracting validated data.")
        file_path = data.get('file_path')
        data_issues = data.get('data_issues', [])
        time_stamp = data.get('timestamp')
        correct_formats = data.get('correct_formats', [])
        criticality = data.get('criticality', "")

        data_errors = []
        data_success = []
        for issue in data_issues:

            data_error_entry = {
                'file_name': file_path,
                'column_name': issue['column'],
                'expectation': issue['expectation'],
                'unexpected_percent': issue['unexpected_percent'],
                'unexpected_index_query': issue['unexpected_index_query'],
                'observed_value': issue['observed_value'],
                'criticality': criticality,
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
            print("Inserting issue.")
            data_errors = []
            for entry in data_to_save:
                error_entry = {
                    'file_name': entry['file_name'],
                    'column_name': entry['column_name'],
                    'expectation': entry['expectation'],
                    'unexpected_percent': entry['unexpected_percent'],
                    'unexpected_index_query': entry['unexpected_index_query'],
                    'observed_value': entry['observed_value'],
                    'criticality': entry['criticality'],
                    'timestamp': entry['timestamp']
                }
                data_error = DataError(**error_entry)
                data_errors.append(data_error)

            session.bulk_save_objects(data_errors)
            session.commit()
            print(f"{len(data_errors)} values inserted successfully.")
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
    def send_alerts(validation_result):
        if validation_result:
            file_path = validation_result['file_path']
            file_name = os.path.basename(file_path).split('.')[0]
            if validation_result['to_split'] == 1:
                file_name = f'{file_name[:-3]}_bad.csv'
                logging.info(f"File name changed to {file_name}")
            else:
                # file_name = f'{file_name[:-3]}.csv'
                logging.info(f'Filename kept as {file_name}')
            file_path = f'{BAD_DATA_DIR}/{file_name}'
            logging.info(f'Resulting file path is in: {file_path}')
            content = ""
            if validation_result['data_issues']:
                timestamp = validation_result.get('timestamp', datetime.now())
                content += f"Timestamp: {timestamp}\n\n"
                content += f"On file: {validation_result['file_path']} found the following issues:\n\n"
                for issue in validation_result['data_issues']:
                    content += f"Expectation: {issue['expectation']}\n\nCriticality: {validation_result['criticality']}\n\n"
                    if issue['column'] != '':
                        content += f"Column: {issue['column']}\n\n\n"
                    content += "-"*30 + "\n"
            else:
                raise AirflowSkipException

            message = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "summary": "Summary",
                "sections": [{
                    "activityTitle": "Data quality report",
                    "activitySubtitle": "--"*40,
                    "facts": [
                        {
                            "name": "\t\t",
                            "value": content
                        }
                    ],
                }],
                "potentialAction": [{
                    "@type": "HttpPOST",
                    "name": "Link to the data docs",
                    "targets": [{
                        "os": "default",
                        "uri": VALIDATION_RESULT_URI
                    }]
                }]
            }

            print("Validation result found: ", content, message)
            response = requests.post(TEAMS_WEBHOOK, json=message)

            if response.status_code == 200:
                logging.info("Alert sent successfully to Microsoft Teams.")
            else:
                logging.error("Failed to send alert to Microsoft Teams:", response.text)

        else:
            logging.warning("No validation found.")

    @task
    def save_data_errors(validation_results: dict) -> None:
        data_issues = validation_results['data_issues']
        if data_issues:

            try:
                engine = db.create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
                Session = sessionmaker(bind=engine)
                data_errors, data_success = extract_values(validation_results)
                session = Session()
                if (data_errors):
                    insert_data_to_database(data_errors, session, is_issue=1)
                if (data_success):
                    insert_data_to_database(data_success, session, is_issue=0)
            except Exception as e:
                print("Error:", e)
        else:
            logging.warning('No data issues in file')
            raise AirflowSkipException

    read_task = read_data()
    validate_task = validate_data(read_task)
    split_and_save_task = split_and_save_data(read_task, validate_task)
    send_alerts_task = send_alerts(validate_task)
    save_data_errors_task = save_data_errors(validate_task)

    read_task >> validate_task >> [split_and_save_task, send_alerts_task, save_data_errors_task]


ingest_wine_data_dag = ingest_wine_data()
