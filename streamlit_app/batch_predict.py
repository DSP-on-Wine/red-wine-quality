import pandas as pd
from models import InputData
from predictor import predict


def batch_predict(df: pd.DataFrame):
    predictions = []
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
        # Make prediction using your model
        prediction = predict(input_data.dict())
        predictions.append(prediction)

    return predictions
