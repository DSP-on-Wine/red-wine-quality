import logging
import pandas as pd
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

ROWS_PER_FILE = 10


@dag(
    dag_id='ingest_wine_data',
    description='Ingest data from winequality-red.csv into multiple files',
    tags=['dsp', 'data_ingestion'],
    schedule_interval=timedelta(days=1),
    start_date=days_ago(n=1)
)
def ingest_wine_data():

    @task
    def split_and_ingest_data() -> None:
        # Read the original CSV file -- replace with input data later
        filepath = '../../data/winequality-red.csv'
        input_data_df = pd.read_csv(filepath)

        # Split the data into chunks of 10 rows each
        num_chunks = len(input_data_df) // ROWS_PER_FILE
        data_chunks = [input_data_df.iloc[
            i:i+ROWS_PER_FILE] 
            for i in range(0, len(input_data_df), ROWS_PER_FILE)]

        # Ingest each chunk of data
        for i, chunk in enumerate(data_chunks):
            save_data(chunk, i)

    @task
    def save_data(chunk: pd.DataFrame, chunk_id: int) -> None:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filepath = f'../output_data/chunk_{chunk_id}_{timestamp}.csv'

        logging.info(f'Ingesting data chunk {chunk_id} to the file: {filepath}')
        chunk.to_csv(filepath, index=False)

    split_and_ingest_data()


ingest_wine_data_dag = ingest_wine_data()
