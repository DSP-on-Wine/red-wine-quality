import requests


def predict(input_data):
    predict_endpoint = "http://localhost:8000/predict"
    response = requests.post(predict_endpoint, json=input_data)
    if response.status_code == 200:
        prediction = response.json()["prediction"]
        return prediction
    else:
        return f"Error: {response.text}"
