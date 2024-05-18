import pandas as pd
import requests
from models import InputData


def batch_predict(df: pd.DataFrame, source: str = "webapp"):
    predictions = []
    input_data_list = []
    ## TODO - add source field with source='webapp' by default
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

    predict_endpoint = "http://localhost:8000/predict"
    response = requests.post(predict_endpoint, json={
        "data": [data.dict() for data in input_data_list],
        "source": source  # Send the source along with the data
    })  


    if response.status_code == 200:
        predictions = response.json()
        prediction_values = [pred['prediction'] for pred in predictions]
        df_with_predictions = df.copy()
        df_with_predictions['prediction'] = prediction_values
        return df_with_predictions

    else:
        return f"Error: {response.text}"
