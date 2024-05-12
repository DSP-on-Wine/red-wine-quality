from airflow.decorators import dag, task
from datetime import datetime, timedelta
import logging
import os
import pandas as pd
from pydantic import BaseModel
import requests


GOOD_DATA_DIR = '/opt/airflow/good_data'

class InputData(BaseModel):
    fixed_acidity: float
    volatile_acidity: float
    citric_acid: float
    residual_sugar: float
    chlorides: float
    free_sulfur_dioxide: float
    total_sulfur_dioxide: float
    density: float
    pH: float
    sulphates: float
    alcohol: float


@dag(schedule_interval=timedelta(days=1), start_date=datetime(2024, 5, 9), catchup=False, tags=['data_ingestion'])
def wine_prediction_dag():

    @task
    def check_for_new_data() -> list:
        good_data_files = os.listdir(GOOD_DATA_DIR)
        if good_data_files:
            logging.info("New ingested files found in good_data directory.")
            return good_data_files
        else:
            logging.info("No new ingested files found in good_data directory.")
            return []

    @task
    def make_predictions(files: list):
        if files:
            for file in files:
                file_path = os.path.join(GOOD_DATA_DIR, file)
                logging.info(f"Making predictions for file: {file_path}")
                df = pd.read_csv(file_path)
                predictions = []
                input_data_list = []
                for index, row in df.iterrows():
                    input_data = InputData(
                        fixed_acidity=row['fixed acidity'],
                        volatile_acidity=row['volatile acidity'],
                        citric_acid=row['citric acid'],
                        residual_sugar=row['residual sugar'],
                        chlorides=row['chlorides'],
                        free_sulfur_dioxide=row['free sulfur dioxide'],
                        total_sulfur_dioxide=row['total sulfur dioxide'],
                        density=row['density'],
                        pH=row['pH'],
                        sulphates=row['sulphates'],
                        alcohol=row['alcohol']
                    )

                    input_data_list.append(input_data)
                    # Logic to make API call to model service and make predictions
                    predict_endpoint = "http://host.docker.internal:8000/predict/"

                    # response = requests.post(model_service_url, json={"file_name": file})
                    response = requests.post(predict_endpoint, json=[
                    data.dict()
                    for data in input_data_list])

                    if response.status_code == 200:
                        predictions = response.json()
                        output = "\n".join([f"{i}: {pred['prediction']}\n"
                                        for i, pred in enumerate(predictions)])

                        return output
                    else:
                        return f"Error: {response.text}"
        else:
            logging.info("No files to make predictions.")

    check_for_new_data_task = check_for_new_data()
    make_predictions_task = make_predictions(check_for_new_data_task)
    check_for_new_data_task >> make_predictions_task

wine_prediction_dag = wine_prediction_dag()




