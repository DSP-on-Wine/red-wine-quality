import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_log_error
from sklearn.linear_model import LinearRegression
import joblib
from .preprocess import preprocess_data
from . import FEATURE_COLS, TARGET_COL, MODEL_PATH


def compute_rmsle(y_test: np.ndarray, y_pred: np.ndarray, precision: int = 2) -> float:
    rmsle = np.sqrt(mean_squared_log_error(y_test, y_pred))
    return round(rmsle, precision)


def build_model(data: pd.DataFrame) -> dict[str, str]:
    X, y = data[FEATURE_COLS], data[TARGET_COL]
    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=42, test_size=0.25)

    X_train_processed = preprocess_data(X_train, 1)
    X_test_processed = preprocess_data(X_test, 0)

    regressor = LinearRegression()
    regressor.fit(X_train_processed, y_train)

    y_pred = regressor.predict(X_test_processed)

    joblib.dump(regressor, MODEL_PATH)

    return {"rmse": str(compute_rmsle(y_test, y_pred))}
