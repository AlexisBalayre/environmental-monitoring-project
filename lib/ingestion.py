# ingestion.py
# The first step of the pipeline

from requests import Session
import datetime as dt
import os


def fetch_sensors_data():
    """
        Fetches the latest data from the sensors and writes it to a CSV file

        Returns:
            now (datetime): The timestamp of the fetched data
    """
    # Fetches the latest data from the data.sensor.community API
    url = "https://data.sensor.community/static/v2/data.24h.json"
    # Use a session to avoid creating a new connection for each request
    session = Session()
    try:
        # Delete all the files in the sensors_data folder
        print("1. Deleting the existing data...")
        for file in os.listdir("data/sensors_data"):
            os.remove(os.path.join("data/sensors_data", file))
        print("Done deleting the existing data.\n")
        # Make the request
        print("2. Fetching the latest data...")
        response = session.get(url)
        # If the response was successful, no Exception will be raised
        if response.status_code == 200 and response.content:
            now = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = os.path.join("data/sensors_data", f"{now}.json")
            with open(file_path, "w") as f:
                f.write(response.text)
            return now
    except Exception as e:
        print(f"Request failed with exception {e}")
    finally:
        session.close()
    return None


# UNIT TEST (UNCOMMENT TO RUN)
timestamp = fetch_sensors_data()
