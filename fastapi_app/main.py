from fastapi import FastAPI
from .models import InputData, Prediction
from .preprocessing import preprocess_data
from . import MODEL_PATH, SCALER_PATH
import joblib


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
