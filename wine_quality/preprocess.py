import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from . import CONTINUOUS_COLS, IMPUTER_PATH, SCALER_PATH
import joblib


def preprocess_data(X: pd.DataFrame, train: bool) -> pd.DataFrame:
    if train == 1:
        scaler = StandardScaler()
        scaler.fit(X[CONTINUOUS_COLS])
        joblib.dump(scaler, SCALER_PATH)
    else:
        scaler = joblib.load(SCALER_PATH)
    X_scaled = scaler.transform(X[CONTINUOUS_COLS])
    return X_scaled
