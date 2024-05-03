from fastapi import FastAPI, HTTPException
from .models import InputData, Prediction
from .preprocessing import preprocess_data
from . import MODEL_PATH, SCALER_PATH, DATABASE_URL
import joblib
import asyncpg
import datetime
from typing import List, Union
import pandas as pd


app = FastAPI()
model = joblib.load(MODEL_PATH)
scaler = joblib.load(SCALER_PATH)


async def connect_to_db():
    return await asyncpg.connect(DATABASE_URL)


async def insert_prediction_into_db(input_data, prediction):
    timestamp = datetime.datetime.now()
    connection = await connect_to_db()
    try:
        await connection.execute(
            """
            INSERT INTO predictions (
                fixed_acidity, volatile_acidity, citric_acid,
                residual_sugar, chlorides, free_sulfur_dioxide,
                total_sulfur_dioxide, density, pH, sulphates,
                alcohol, prediction, timestamp
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
            $9, $10, $11, $12, $13)
            """,
            input_data.fixed_acidity, input_data.volatile_acidity,
            input_data.citric_acid, input_data.residual_sugar,
            input_data.chlorides, input_data.free_sulfur_dioxide,
            input_data.total_sulfur_dioxide, input_data.density,
            input_data.pH, input_data.sulphates, input_data.alcohol,
            prediction, timestamp
        )
    finally:
        await connection.close()


@app.post("/predict/")
async def predict(data: Union[InputData, List[InputData]]):
    if isinstance(data, InputData):
        data = [data]

    predictions = []
    for input_data in data:
        processed_data = preprocess_data(input_data, scaler)
        prediction = model.predict(processed_data)
        prediction_response = Prediction(prediction=prediction[0])

        await insert_prediction_into_db(input_data, prediction[0])

        predictions.append(prediction_response)

    return predictions


@app.get("/get_past_predictions/")
async def get_past_predictions(start_date: datetime.datetime,
                               end_date: datetime.datetime):
    end_date = end_date.replace(hour=23, minute=59,
                                second=59, microsecond=999999)
    connection = await connect_to_db()
    try:
        query = """
            SELECT *
            FROM predictions
            WHERE timestamp >= $1 AND timestamp <= $2
        """
        rows = await connection.fetch(query, start_date, end_date)
        if not rows:
            raise HTTPException(
                status_code=404,
                detail="No data found between the specified dates")

        df = pd.DataFrame(rows, columns=[
            "fixed_acidity", "volatile_acidity", "citric_acid",
            "residual_sugar", "chlorides", "free_sulfur_dioxide",
            "total_sulfur_dioxide", "density", "pH", "sulphates",
            "alcohol", "prediction", "timestamp"
        ])

        df['timestamp'] = pd.to_datetime(df['timestamp'])

        return df
    finally:
        await connection.close()
