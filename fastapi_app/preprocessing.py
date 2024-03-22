from . import FEATURE_COLS
from .models import InputData


def preprocess_data(data: InputData, scaler):
    scaled_data = scaler.transform(data[FEATURE_COLS])
    return scaled_data
