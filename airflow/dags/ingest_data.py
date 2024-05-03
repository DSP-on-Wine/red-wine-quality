<<<<<<< HEAD
#!/Users/LEGION/miniconda3/envs/dsp_proj/bin/python

import logging
import os
import random
from datetime import datetime, timedelta
=======
import logging
import os
import random
from datetime import timedelta
>>>>>>> 918a146816d2cb94bff7d8b184e2bc15677387e0
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import shutil

<<<<<<< HEAD
RAW_DATA_DIR = 'raw_data'
GOOD_DATA_DIR = 'good_data'

=======

RAW_DATA_DIR = 'raw_data'
GOOD_DATA_DIR = 'good_data'


>>>>>>> 918a146816d2cb94bff7d8b184e2bc15677387e0
def move_file(file_path: str, target_dir: str) -> None:
    if os.path.exists(file_path):
        file_name = os.path.basename(file_path)
        target_path = os.path.join(target_dir, file_name)
        shutil.move(file_path, target_path)
        logging.info(f"Moved file {file_name} to {target_dir}.")
    else:
        logging.info(f"File {file_path} does not exist.")

<<<<<<< HEAD
=======

>>>>>>> 918a146816d2cb94bff7d8b184e2bc15677387e0
@dag(
    dag_id='ingest_wine_data',
    description='Move a randomly selected file from raw-data to good-data',
    tags=['data_movement', 'dsp', 'data_ingestion'],
    schedule_interval=timedelta(days=1),
<<<<<<< HEAD
    start_date=days_ago(n=1)    
=======
    start_date=days_ago(n=1)
>>>>>>> 918a146816d2cb94bff7d8b184e2bc15677387e0
)
def ingest_wine_data():

    @task
    def read_data() -> str:
<<<<<<< HEAD
        # List all files in the raw data directory
        raw_files = os.listdir(RAW_DATA_DIR)
        
        if raw_files:
            # Select a random file from the list
=======
        raw_files = os.listdir(RAW_DATA_DIR)

        if raw_files:
>>>>>>> 918a146816d2cb94bff7d8b184e2bc15677387e0
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
<<<<<<< HEAD
        # if file_path:
        #     # Move the selected file to the good data directory
        #     file_name = os.path.basename(file_path)
        #     new_file_path = os.path.join(GOOD_DATA_DIR, file_name)
        #     os.rename(file_path, new_file_path)
        #     logging.info(f"Moved file {file_name} from raw-data to good-data.")
        # else:
        #     logging.info("No file to save.")

    # Define the task dependency
    file_path = read_data()
    save_file(file_path)

=======

    file_path = read_data()
    save_file(file_path)


>>>>>>> 918a146816d2cb94bff7d8b184e2bc15677387e0
ingest_wine_data_dag = ingest_wine_data()
