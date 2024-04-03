import logging
import os
import random
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import shutil


RAW_DATA_DIR = 'raw_data'
GOOD_DATA_DIR = 'good_data'


def move_file(file_path: str, target_dir: str) -> None:
    if os.path.exists(file_path):
        file_name = os.path.basename(file_path)
        target_path = os.path.join(target_dir, file_name)
        shutil.move(file_path, target_path)
        logging.info(f"Moved file {file_name} to {target_dir}.")
    else:
        logging.info(f"File {file_path} does not exist.")


@dag(
    dag_id='ingest_wine_data',
    description='Move a randomly selected file from raw-data to good-data',
    tags=['data_movement', 'dsp', 'data_ingestion'],
    schedule_interval=timedelta(days=1),
    start_date=days_ago(n=1)
)
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
    def save_file(file_path: str) -> None:
        if file_path:
            move_file(file_path, GOOD_DATA_DIR)
        else:
            logging.info("No file to save.")

    file_path = read_data()
    save_file(file_path)


ingest_wine_data_dag = ingest_wine_data()
