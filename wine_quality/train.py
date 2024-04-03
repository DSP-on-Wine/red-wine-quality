import pandas as pd
from sklearn.metrics import mean_squared_log_error
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from .preprocess import preprocess_data
import numpy as np
import joblib
from . import FEATURE_COLS, TARGET_COL, MODEL_PATH


def compute_rmse(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    precision: int = 2
) -> float:
    rmsle = np.sqrt(mean_squared_log_error(y_true, y_pred))
    return round(rmsle, precision)


def build_model(data: pd.DataFrame) -> dict:
    X, y = data[FEATURE_COLS], data[TARGET_COL]

    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2,
                                                      random_state=42)

    X_train_processed = preprocess_data(X_train, 1)
    X_val_processed = preprocess_data(X_val, 0)

    model = RandomForestRegressor(random_state=42)
    model.fit(X_train_processed, y_train)
    joblib.dump(model, MODEL_PATH)

    predictions = model.predict(X_val_processed)

    return {'rmlse': compute_rmse(y_val, predictions)}

