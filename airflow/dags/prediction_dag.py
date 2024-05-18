from airflow.decorators import dag, task
from datetime import datetime, timedelta
import logging
import os
import pandas as pd
from pydantic import BaseModel
import requests
from sqlalchemy import create_engine, Column, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from airflow.exceptions import AirflowSkipException

# TODO:
# see how to read DB info from .env file
# try to make it select multiple files at once


GOOD_DATA_DIR = '/opt/airflow/good_data'

# Define SQLAlchemy model
Base = declarative_base()


class OldFile(Base):
    __tablename__ = 'old_files'

    filename = Column(String, primary_key=True)


# Database connection
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'
DB_HOST = 'host.docker.internal'
DB_PORT = '5432'
DB_NAME = 'wine_quality'

engine = create_engine(
    f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    )
Session = sessionmaker(bind=engine)


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


@dag(
        schedule_interval=timedelta(seconds=30),
        start_date=datetime(2024, 5, 18),
        catchup=False,
        tags=['data_ingestion']
        )
def wine_prediction_dag():

    @task
    def check_for_new_data() -> list:
        session = Session()
        try:
            # Query existing files from the database
            existing_files = [
                file.filename for file in session.query(OldFile).all()
                ]
            good_data_files = os.listdir(GOOD_DATA_DIR)
            new_files = [
                file for file in good_data_files if file not in existing_files
                ]
            if new_files:
                logging.info(
                    "New ingested files found in good_data directory."
                    )
            else:
                logging.info(
                    "No new ingested files found in good_data directory."
                    )
                raise AirflowSkipException
            return new_files
        finally:
            session.close()

    @task
    def make_predictions(files: list):
        output_list = []  # Accumulate output for all files
        if files:
            session = Session()
            try:
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

                    source = 'scheduled predictions'
                    predict_endpoint = "http://host.docker.internal:8000/predict/"
                    response = requests.post(predict_endpoint, json={
                        "data": [data.dict() for data in input_data_list],
                        "source": source  # Send the source along with the data
                        })

                    if response.status_code == 200:
                        predictions = response.json()
                        output = "\n".join(
                            [f"{i}: {pred['prediction']}\n"
                             for i, pred in enumerate(predictions)]
                                            )

                        # Save file name in the database
                        session.add(OldFile(filename=file))
                        session.commit()
                        # Append output for this file
                        output_list.append(output)
                    else:
                        output_list.append(f"Error: {response.text}")

            except Exception as e:
                # Log the exception and append error message to output list
                logging.error(f"Error making predictions: {e}")
                output_list.append(f"Error making predictions: {e}")

            finally:
                session.close()
        else:
            logging.info("No files to make predictions.")
            raise AirflowSkipException

    check_for_new_data_task = check_for_new_data()
    make_predictions_task = make_predictions(check_for_new_data_task)
    check_for_new_data_task >> make_predictions_task


wine_prediction_dag = wine_prediction_dag()
