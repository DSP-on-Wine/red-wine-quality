from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from great_expectations.data_asset import DataAsset
from great_expectations.dataset import PandasDataset
from airflow.operators.python_operator import PythonOperator

RAW_DATA_DIR = 'raw_data'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'great_expectations_setup',
    default_args=default_args,
    tags=['data_ingestion'],
    description='Setup Great Expectations in Airflow',
    schedule_interval=timedelta(days=1),
)

def test_great_expectations():
    file_path = f'{RAW_DATA_DIR}/data_chunk_1.csv'
    data_asset = DataAsset(PandasDataset(file_path))
    # Perform some basic validation using Great Expectations
    # For example:
    # validation_result = data_asset.validate()
    # print(validation_result)

test_great_expectations_task = PythonOperator(
    task_id='test_great_expectations',
    python_callable=test_great_expectations,
    dag=dag,
)

test_great_expectations_task






# import os
# import random
# import shutil
# import logging
# from datetime import timedelta
# from airflow import DAG
# from airflow.decorators import dag, task
# from airflow.utils.dates import days_ago
# from great_expectations.core import ExpectationSuiteValidationResult
# from great_expectations.data_asset import DataAsset
# from great_expectations.dataset import PandasDataset
# from airflow.operators.python_operator import PythonOperator

# RAW_DATA_DIR = 'raw_data'
# GOOD_DATA_DIR = 'good_data'
# BAD_DATA_DIR = 'bad_data'

# # Task to move a file from one location to another
# def move_file(file_path: str, target_dir: str) -> None:
#     if os.path.exists(file_path):
#         file_name = os.path.basename(file_path)
#         target_path = os.path.join(target_dir, file_name)
#         shutil.move(file_path, target_path)
#         logging.info(f"Moved file {file_name} to {target_dir}.")
#     else:
#         logging.info(f"File {file_path} does not exist.")

# # Task to validate the data using Great Expectations
# def validate_data(**kwargs):
#     file_path = kwargs['ti'].xcom_pull(task_ids='task_that_produces_file_path')
#     if os.path.exists(file_path):
#         data_asset = DataAsset(PandasDataset(file_path))
#         # batch_kwargs = BatchKwargs({"path": file_path})
#         # validation_result = data_asset.validate(expectation_suite_name="my_expectations", batch_kwargs=batch_kwargs)
#         # return validation_result
#     else:
#         return None

# @dag(
#     dag_id='data_ingestion',
#     description='Data Ingestion Job',
#     tags=['data_ingestion'],
#     schedule_interval=timedelta(days=1),
#     start_date=days_ago(n=1)
# )
# def data_ingestion_dag():
#     @task
#     def read_data() -> str:
#         raw_files = os.listdir(RAW_DATA_DIR)
#         if raw_files:
#             random_file = random.choice(raw_files)
#             file_path = os.path.join(RAW_DATA_DIR, random_file)
#             logging.info(f"Selected file {random_file} from raw-data.")
#             return file_path
#         else:
#             logging.info("No files found in raw-data directory.")
#             return None

#     @task
#     def send_alerts(validation_result: ExpectationSuiteValidationResult) -> None:
#         # Assuming these functions are defined elsewhere
#         # generate_data_docs(validation_result)
#         # get_criticality(validation_result)
#         # get_errors_summary(validation_result)
#         # get_report_link(validation_result)
#         # teams_alert(criticality, errors_summary, report_link)
#         pass

#     @task
#     def split_and_save_data(file_path: str, validation_result: ExpectationSuiteValidationResult) -> None:
#         if validation_result.success:
#             move_file(file_path, GOOD_DATA_DIR)
#         else:
#             pass
#             # errors_df = get_errors_dataframe(validation_result)
#             # if errors_df.empty:
#             #     move_file(file_path, BAD_DATA_DIR)
#             # else:
#                 # good_data_df = filter_good_data(errors_df)
#                 # bad_data_df = filter_bad_data(errors_df)

#                 # if not good_data_df.empty:
#                 #     move_file(file_path, GOOD_DATA_DIR)

#                 # if not bad_data_df.empty:
#                     # move_file(file_path, BAD_DATA_DIR)

#     @task
#     def save_data_errors(validation_result: ExpectationSuiteValidationResult) -> None:
#         # errors_df = get_errors_dataframe(validation_result)
#         # Assuming this function saves errors to database
#         # save_errors_to_database(errors_df)
#         pass

#     file_path = read_data()
#     validation_result = validate_data(file_path)

#     send_alerts(validation_result)
#     split_and_save_data(file_path, validation_result)
#     save_data_errors(validation_result)

# data_ingestion_dag = data_ingestion_dag()
