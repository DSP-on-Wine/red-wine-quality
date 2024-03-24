import streamlit as st
import pandas as pd
import requests
from pydantic import BaseModel

# Load the dataset
@st.cache_data
def load_data():
    return pd.read_csv('data\\winequality-red.csv')

# Function to make prediction
def predict(input_data):
    predict_endpoint = "http://localhost:8000/predict"
    response = requests.post(predict_endpoint, json=input_data)
    if response.status_code == 200:
        prediction = response.json()["prediction"]
        return prediction
    else:
        return f"Error: {response.text}"

# Define the Pydantic input data model
class InputData(BaseModel):
    fixed_acidity: float
    volatile_acidity: float
    citric_acid: float
    residual_sugar: float
    chlorides: float
    free_sulfur_dioxide: float
    total_sulfur_dioxide: float
    density: float
    pH: float
    sulphates: float
    alcohol: float

# Main function to define the dashboard layout and components
def main():
    st.title('Red Wine Quality Predictor')

    # Load the dataset
    data = load_data()

    # Display the dataset
    st.subheader('Dataset')
    st.dataframe(data)

    # Sidebar with user inputs
    st.sidebar.header('User Input Features')

    # Define user input fields for numerical features
    fixed_acidity = st.sidebar.slider('Fixed Acidity', min_value=0.0, max_value=15.0, value=7.0)
    volatile_acidity = st.sidebar.slider('Volatile Acidity', min_value=0.0, max_value=2.0, value=0.5)
    citric_acid = st.sidebar.slider('Citric Acid', min_value=0.0, max_value=1.0, value=0.0)
    residual_sugar = st.sidebar.slider('Residual Sugar', min_value=0.0, max_value=15.0, value=2.0)
    chlorides = st.sidebar.slider('Chlorides', min_value=0.0, max_value=1.0, value=0.08)
    free_sulfur_dioxide = st.sidebar.slider('Free Sulfur Dioxide', min_value=0.0, max_value=100.0, value=10.0)
    total_sulfur_dioxide = st.sidebar.slider('Total Sulfur Dioxide', min_value=0.0, max_value=300.0, value=30.0)
    density = st.sidebar.slider('Density', min_value=0.0, max_value=2.0, value=1.0)
    pH = st.sidebar.slider('pH', min_value=0.0, max_value=10.0, value=3.5)
    sulphates = st.sidebar.slider('Sulphates', min_value=0.0, max_value=2.0, value=0.5)
    alcohol = st.sidebar.slider('Alcohol', min_value=8.0, max_value=16.0, value=10.0)

    # Button to trigger prediction
    if st.sidebar.button('Predict'):
        input_data = {
            'fixed_acidity': fixed_acidity,
            'volatile_acidity': volatile_acidity,
            'citric_acid': citric_acid,
            'residual_sugar': residual_sugar,
            'chlorides': chlorides,
            'free_sulfur_dioxide': free_sulfur_dioxide,
            'total_sulfur_dioxide': total_sulfur_dioxide,
            'density': density,
            'pH': pH,
            'sulphates': sulphates,
            'alcohol': alcohol
        }

        prediction = predict(input_data)

        st.sidebar.subheader('Prediction')
        st.sidebar.write(prediction)

if __name__ == '__main__':
    main()
