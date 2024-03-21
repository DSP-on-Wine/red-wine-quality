import pandas as pd
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from .preprocess import preprocess_data
import numpy as np
import joblib


def compute_rmse(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    precision: int = 2
) -> float:
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    return round(rmse, precision)


def build_model(data: pd.DataFrame) -> dict:
    X = data.drop('quality', axis=1)
    y = data['quality']

    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2,
                                                      random_state=42)

    X_train_processed = preprocess_data(X_train, 1)
    X_val_processed = preprocess_data(X_val, 0)

    model = RandomForestRegressor(random_state=42)
    model.fit(X_train_processed, y_train)
    joblib.dump(model, '../models/model.joblib')

    predictions = model.predict(X_val_processed)

    return {'rmse': compute_rmse(y_val, predictions)}
