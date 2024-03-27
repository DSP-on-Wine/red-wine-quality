import pandas as pd
import requests
from models import InputData


def batch_predict(df: pd.DataFrame):
    df.columns = df.columns.str.replace(' ', '_')
    input_data_list = []
    for _, row in df.iterrows():
        input_data = InputData(**row.to_dict())
        input_data_list.append(input_data)

    predictions = []

    # Send a POST request to the predict_batch endpoint
    endpoint_url = "http://localhost:8000/predict_batch/"
    data_json = [data.dict() for data in input_data_list]
    response = requests.post(endpoint_url, json=data_json)
    if response.status_code == 200:
        predictions_dict = response.json()
        for _, pred in enumerate(predictions_dict):
            predictions.append(pred['prediction'])
    else:
        return f"Error: {response.text}"

    return predictions
