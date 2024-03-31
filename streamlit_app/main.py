import pandas as pd
import streamlit as st
from models import InputData
from predictor import predict
from data_loader import load_data
from sidebar import add_sidebar
from batch_predict import batch_predict
from get_predictions import get_predictions
import datetime

def past_predictions_page():
    st.title('Past Predictions')

    start_date = st.date_input('Start Date', datetime.date.today() - datetime.timedelta(days=7))
    end_date = st.date_input('End Date', datetime.date.today())

    if start_date <= end_date:
        if st.button('Get Past Predictions'):
            past_predictions_data = get_predictions(start_date, end_date)

            if not isinstance(past_predictions_data, str):
                st.subheader('Past Predictions')
                st.dataframe(past_predictions_data)
            else:
                st.error(past_predictions_data)
    else:
        st.error('Error: End date must be after or equal to start date.')


def main():
    st.title('Red Wine Quality Predictor')
    # File uploader for CSV file
    page = st.sidebar.selectbox("Select a page", ["Home", "Past Predictions"])

    if page == "Home":
        uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])
        if uploaded_file is not None:
            df = pd.read_csv(uploaded_file)
            st.subheader('Uploaded Data')
            st.write(df)

            if st.button('Predict for batch'):
                predictions = batch_predict(df)
                st.subheader('Predictions')
                st.write(predictions)

        data = load_data()

        st.subheader('Dataset used for training')
        st.dataframe(data)

        fixed_acidity, volatile_acidity, citric_acid, residual_sugar, \
            chlorides, free_sulfur_dioxide, total_sulfur_dioxide, density, \
            pH, sulphates, alcohol = add_sidebar()

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
                alcohol=alcohol
            )
            prediction = predict(input_data.dict())

            st.sidebar.subheader('Prediction')
            st.sidebar.write(prediction)

    elif page == "Past Predictions":
        # Render past predictions page
        past_predictions_page()


if __name__ == '__main__':
    main()
