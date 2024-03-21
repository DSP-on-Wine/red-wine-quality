import pandas as pd
import numpy as np
from .preprocess import preprocess_data
from . import FEATURE_COLS, MODEL_PATH
import joblib


def make_predictions(input_data: pd.DataFrame) -> np.ndarray:
    test_data_selected = input_data[FEATURE_COLS]

    test_data_processed = preprocess_data(test_data_selected, 0)
    regressor = joblib.load(MODEL_PATH)
    wine_quality_pred = regressor.predict(test_data_processed)

    return wine_quality_pred
