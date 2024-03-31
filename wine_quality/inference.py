import pandas as pd
import numpy as np
from .preprocess import preprocess_data
from . import FEATURE_COLS, MODEL_PATH
import joblib


def make_predictions(input_data: pd.DataFrame) -> np.ndarray:
    X_processed = preprocess_data(input_data[FEATURE_COLS], 0)
    model = joblib.load(MODEL_PATH)
    predictions = model.predict(X_processed)

    return predictions

