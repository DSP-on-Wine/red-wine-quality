import pandas as pd
import numpy as np
from .preprocess import preprocess_data
import joblib


def make_predictions(input_data: pd.DataFrame) -> np.ndarray:
    X_processed = preprocess_data(input_data, 0)

    model = joblib.load('../models/model.joblib')
    predictions = model.predict(X_processed)

    return predictions
