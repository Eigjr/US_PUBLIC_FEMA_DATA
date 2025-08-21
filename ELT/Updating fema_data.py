import os
import json
import pandas as pd
import requests
from datetime import datetime
from dotenv import load_dotenv
from config import URL

# Load environment variables from .env file
load_dotenv()
# This code is meant to update the pervious extract.py dataset to capture changes that have taken place over the time

# File path to store the last loaded timestamp
file_path = os.getenv("update_path")

print("file path load successfully!")


def get_last_loaded_time():
    """
    Load the last refresh time from JSON file.
    Returns a default value if the file is not found or corrupt.
    """
    try:
        with open(file_path, "r") as load_file:
            return json.load(load_file)["lastRefresh"]
    except (FileNotFoundError, json.JSONDecodeError):
        return "2025-01-20T15:02:16.829Z"  # Default start time

def save_last_loaded_time(timestamp):
    """
    Save the latest timestamp to the JSON file.
    """
    with open(file_path, "w") as file:
        json.dump({"lastRefresh": timestamp}, file)

def extract_data():
    """
    Fetch new/updated FEMA data and merge with local CSV.
    """
    last_refresh = get_last_loaded_time()
    filter_param = f"?$filter=lastRefresh%20ge%20{last_refresh}"
    full_url = URL+filter_param


    try:
        print("Fetching from:", full_url)
        response = requests.get(full_url)
        response.raise_for_status()  # Raise error if HTTP request failed
        
        print("fetch successfully")


        data = response.json()

        print(data.keys())
        print(data["metadata"])
        print(data["PublicAssistanceFundedProjectsDetails"])

        if "PublicAssistanceFundedProjectsDetails" not in data or not data["PublicAssistanceFundedProjectsDetails"]:
            
            
            print("No new data found.")
        
        
            return None

        # Convert to DataFrame
        df = pd.json_normalize(data["PublicAssistanceFundedProjectsDetails"])

        # Check if file exists
        if os.path.exists("fema_data.csv"):
            existing_df = pd.read_csv("fema_data.csv")
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df.drop_duplicates(subset=["id"], inplace=True)
            combined_df.to_csv("fema_data.csv", index=False)
            print("New data appended to existing file.")
        else:
            df.to_csv("fema_data.csv", index=False)
            print(" File created with first batch of data.")

        # Save current time as new "last loaded"
        new_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        save_last_loaded_time(new_time)

        return df

    except requests.exceptions.RequestException as e:
        print(f" Error fetching data: {e}")
        return None

# Run the extraction
extract_data()
