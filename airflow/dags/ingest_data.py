import logging
import os
import random
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

# Define the absolute paths to raw_data and good_data directories
RAW_DATA_DIR = '/sources/raw_data'
GOOD_DATA_DIR = 'good_data'

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
        # List all files in the raw data directory
        raw_files = os.listdir(RAW_DATA_DIR)
        
        if raw_files:
            # Select a random file from the list
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
            # Move the selected file to the good data directory
            file_name = os.path.basename(file_path)
            new_file_path = os.path.join(GOOD_DATA_DIR, file_name)
            os.rename(file_path, new_file_path)
            logging.info(f"Moved file {file_name} from raw-data to good-data.")
        else:
            logging.info("No file to save.")

    # Define the task dependency
    file_path = read_data()
    save_file(file_path)

ingest_wine_data_dag = ingest_wine_data()
