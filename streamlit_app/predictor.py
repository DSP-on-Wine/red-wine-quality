import requests
from models import InputData

def predict(input_data, source: str = "webapp"):
    if isinstance(input_data, InputData):
        input_data = input_data.dict()
    
    # Ensure the input data is wrapped in a list to match the endpoint's expected structure
    if not isinstance(input_data, list):
        input_data = [input_data]

    predict_endpoint = "http://localhost:8000/predict"
    response = requests.post(predict_endpoint, json={
        "data": input_data,
        "source": source
        })
    if response.status_code == 200:
        prediction = response.json()[0]["prediction"]
        return prediction
    else:
        return f"Error: {response.text}"
