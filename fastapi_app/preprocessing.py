import pandas as pd
import numpy as np
from .models import InputData


def preprocess_data(data: InputData, scaler) -> np.ndarray:
    # Create a dictionary with attribute names as keys and their values
    data_dict = {
        "fixed acidity": data.fixed_acidity,
        "volatile acidity": data.volatile_acidity,
        "citric acid": data.citric_acid,
        "residual sugar": data.residual_sugar,
        "chlorides": data.chlorides,
        "free sulfur dioxide": data.free_sulfur_dioxide,
        "total sulfur dioxide": data.total_sulfur_dioxide,
        "density": data.density,
        "pH": data.pH,
        "sulphates": data.sulphates,
        "alcohol": data.alcohol
    }

    # Convert dictionary to DataFrame
    data_df = pd.DataFrame(data_dict, index=[0])

    # Scale the data using the scaler
    scaled_data = scaler.transform(data_df)

    return scaled_data
