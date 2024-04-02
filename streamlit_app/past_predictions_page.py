import streamlit as st
from get_predictions import get_predictions
import datetime


def past_predictions_page():
    st.title('Past Predictions')

    today = datetime.date.today()
    week_ago = today - datetime.timedelta(days=7)
    start_date = st.date_input('Start Date', week_ago)
    end_date = st.date_input('End Date', today)

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
