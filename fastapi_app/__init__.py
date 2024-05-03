<<<<<<< HEAD
=======
import os
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

>>>>>>> 918a146816d2cb94bff7d8b184e2bc15677387e0
ENCODER_PATH = './models/encoder.joblib'
SCALER_PATH = './models/scaler.joblib'
IMPUTER_PATH = './models/imputer.joblib'
MODEL_PATH = './models/model.joblib'
<<<<<<< HEAD
DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/wine_quality"
=======
>>>>>>> 918a146816d2cb94bff7d8b184e2bc15677387e0
