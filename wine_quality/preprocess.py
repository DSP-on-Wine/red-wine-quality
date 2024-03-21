import pandas as pd
from sklearn.preprocessing import StandardScaler
import numpy as np
import joblib

NUMERICAL_FEATURES = ['fixed acidity', 'volatile acidity', 'citric acid',
                      'residual sugar', 'chlorides', 'free sulfur dioxide',
                      'total sulfur dioxide', 'density', 'pH', 'sulphates',
                      'alcohol']


def preprocess_data(data: pd.DataFrame, is_train: bool) -> np.ndarray:
    scaler = StandardScaler()
    if is_train:
        scaler.fit(data[NUMERICAL_FEATURES])
        dump_scaler(scaler, '../models/')
    else:
        scaler = load_scaler('../models/')

    X_scaled = scaler.transform(data[NUMERICAL_FEATURES])
    return X_scaled


def dump_scaler(scaler, model_path: str) -> None:
    joblib.dump(scaler, f'{model_path}/scaler.joblib')


def load_scaler(model_path: str):
    scaler = joblib.load(f'{model_path}/scaler.joblib')
    return scaler
