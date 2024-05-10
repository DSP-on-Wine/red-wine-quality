import pandas as pd
import streamlit as st
from models import InputData
from data_loader import load_data
from sidebar import add_sidebar
from batch_predict import batch_predict
from predictor import predict


def upload_csv():
    uploaded_file = st.file_uploader("Upload CSV file:", type=["csv"])
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        st.subheader('Uploaded Data')
        st.write(df)
        return df
    return None


def display_predictions(df, predictions):
    st.subheader('Predictions')
    # Merge the predictions with the initial CSV input
    if not df.empty and len(predictions) == len(df):
        df['Prediction'] = predictions
    st.write(df)
    ## TODO - take inputs test data + predictions 
    ## TODO - merge predictions with initial csv input for test, write whole df
    ##st.write(predictions)


def predict_batch(df):
    if st.button('Predict for batch'):
        predictions = batch_predict(df)
        display_predictions(df, predictions) 
        ## TODO - send df as input


def display_training_data():
    data = load_data()
    st.subheader('Dataset used for training')
    st.dataframe(data)


def predict_single():
    fixed_acidity, volatile_acidity, citric_acid, residual_sugar, \
        chlorides, free_sulfur_dioxide, total_sulfur_dioxide, density, \
        pH, sulphates, alcohol = add_sidebar()

    # TODO - send the source of the prediction with source=webapp by default
    # Add a source of prediction
    source = st.sidebar.selectbox('Select Source', ['WebApp', 'Other'], index=0)
    if st.sidebar.button('Predict'):
        input_data = InputData(
            fixed_acidity=fixed_acidity,
            volatile_acidity=volatile_acidity,
            citric_acid=citric_acid,
            residual_sugar=residual_sugar,
            chlorides=chlorides,
            free_sulfur_dioxide=free_sulfur_dioxide,
            total_sulfur_dioxide=total_sulfur_dioxide,
            density=density,
            pH=pH,
            sulphates=sulphates,
            alcohol=alcohol,
            source=source
        )
        prediction = predict(input_data.dict())
        st.sidebar.subheader('Prediction')
        st.sidebar.write(prediction)


def predict_page():
    st.title('Predict')

    df = upload_csv()
    if df is not None:
        predict_batch(df)

    display_training_data()
    predict_single()
