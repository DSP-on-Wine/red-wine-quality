import pandas as pd
from sklearn.preprocessing import StandardScaler
import numpy as np
import joblib
from . import NUMERICAL_FEATURES, SCALER_PATH


def preprocess_data(data: pd.DataFrame, is_train: bool) -> np.ndarray:
    scaler = StandardScaler()
    if is_train:
        scaler.fit(data[NUMERICAL_FEATURES])
        joblib.dump(scaler, SCALER_PATH)
    else:
        scaler = joblib.load(SCALER_PATH)

    X_scaled = scaler.transform(data[NUMERICAL_FEATURES])
    return X_scaled