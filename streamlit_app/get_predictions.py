import pandas as pd
import datetime
import requests


def get_predictions(start_date: datetime.datetime,
                    end_date: datetime.datetime,
                    source):
    endpoint_url = "http://localhost:8000/get_past_predictions/"
    response = requests.get(endpoint_url, params={
        "start_date": start_date,
        "end_date": end_date,
        "source": source})

    if response.status_code == 200:
        df = pd.DataFrame(response.json())
        return df

    else:
        return f"Error: {response.text}"
