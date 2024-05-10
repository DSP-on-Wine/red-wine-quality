import pandas as pd
import datetime
import requests

## TODO - add source as input
def get_predictions(start_date: datetime.datetime,
                    end_date: datetime.datetime, souce: str= None):
    endpoint_url = "http://localhost:8000/get_past_predictions/"
    response = requests.get(endpoint_url, params={
        "start_date": start_date.isformat(),
        "end_date": end_date.isformat()})
    ## TODO - add get for 'source' /// might not be necessary

    if source: 
        params['source'] = source 
    
    if response.status_code == 200:
        df = pd.DataFrame(response.json())
        return df

    else:
        return f"Error: {response.text}"
