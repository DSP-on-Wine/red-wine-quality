from fastapi import FastAPI, HTTPException
from .models import InputData, Prediction
from .preprocessing import preprocess_data
from . import MODEL_PATH, SCALER_PATH
import joblib
import asyncpg
import datetime
from typing import List

DATABASE_URL = "postgresql://postgres:123@localhost:5432/wine_quality_predictions"

app = FastAPI()
model = joblib.load(MODEL_PATH)
scaler = joblib.load(SCALER_PATH)


# Define prediction endpoint
@app.post("/predict/")
def predict(data: InputData):
    processed_data = preprocess_data(data, scaler)
    prediction = model.predict(processed_data)
    prediction_response = Prediction(prediction=prediction[0])
    return prediction_response

async def connect_to_db():
    return await asyncpg.connect(DATABASE_URL)


async def insert_prediction(prediction_value, input_data):
    timestamp = datetime.datetime.now()
    connection = await connect_to_db()  # Await the coroutine here
    try:
        await connection.execute(
            """
            INSERT INTO predictions (
                fixed_acidity, volatile_acidity, citric_acid, residual_sugar,
                chlorides, free_sulfur_dioxide, total_sulfur_dioxide, density,
                pH, sulphates, alcohol, prediction, timestamp
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            """,
            input_data.fixed_acidity, input_data.volatile_acidity,
            input_data.citric_acid, input_data.residual_sugar,
            input_data.chlorides, input_data.free_sulfur_dioxide,
            input_data.total_sulfur_dioxide, input_data.density,
            input_data.pH, input_data.sulphates, input_data.alcohol,
            prediction_value, timestamp
        )
    finally:
        await connection.close()  # Close the connection after usage


# Define endpoint to save predictions into the database
@app.post("/save_prediction/")
async def save_prediction(data: InputData, prediction: Prediction):
    await insert_prediction(prediction.prediction, data)
    return {"message": "Prediction saved successfully."}

# Endpoint for get past predictions
@app.get("/get_past_predictions/", response_model=List[Prediction])
async def get_past_predictions(start_date: datetime.datetime, end_date: datetime.datetime) -> List[Prediction]:
    connection = await connect_to_db()  # Await the coroutine here
    try:
        query = """
            SELECT * FROM predictions
            WHERE timestamp >= $1 AND timestamp <= $2
        """
        rows = await connection.fetch(query, start_date, end_date)
        predictions = [Prediction(**row) for row in rows]
        if not predictions:
            raise HTTPException(status_code=404, detail="No data found between the specified dates")
        return predictions
    finally:
        await connection.close()  # Close the connection after usage

