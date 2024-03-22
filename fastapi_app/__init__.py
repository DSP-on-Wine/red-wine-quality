TARGET_COL = 'quality'
ENCODER_PATH = '../models/encoder.joblib'
SCALER_PATH = '../models/scaler.joblib'
IMPUTER_PATH = '../models/imputer.joblib'
MODEL_PATH = '../models/model.joblib'
CATEGORICAL_FEATURES = []
NUMERICAL_FEATURES = ['fixed acidity', 'volatile acidity', 'citric acid',
                      'residual sugar', 'chlorides', 'free sulfur dioxide',
                      'total sulfur dioxide', 'density', 'pH', 'sulphates',
                      'alcohol']
FEATURE_COLS = CATEGORICAL_FEATURES + NUMERICAL_FEATURES
