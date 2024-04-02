import streamlit as st
from past_predictions_page import past_predictions_page
from predict_page import predict_page


def main():
    st.title('Red Wine Quality Predictor')
    page = st.sidebar.selectbox("Select a page",
                                ["Predict", "Past Predictions"])

    if page == "Predict":
        predict_page()

    elif page == "Past Predictions":
        past_predictions_page()


if __name__ == '__main__':
    main()
