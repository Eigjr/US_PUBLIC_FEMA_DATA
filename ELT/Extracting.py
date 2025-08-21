import requests
import pandas as pd
from config import URL

def extract_data():
    """
    Extracts data from the FEMA API.
    
    Returns:
        dict: The JSON response from the API.
    """
    try:
        response = requests.get(URL)
        response.raise_for_status()  # Raise an error for bad responses
        data = response.json()
      
        #save to csv
        dp= pd.json_normalize(response.json())
        dp.to_csv('fema_data.csv', index=False) 
        return data

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data: {e}")
        return None
    

extract_data()
