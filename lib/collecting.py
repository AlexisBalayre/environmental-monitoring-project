# collecting.py
# The first step of the pipeline

from requests import Session
import datetime as dt


def fetch_sensors_data(sparkSession):
    """
    Fetches the latest data from the sensors and returns it as a Spark DataFrame

    Args:
        sparkSession (SparkSession): The SparkSession instance

    Returns:
        df (DataFrame): The DataFrame containing the last data from the sensors
    """

    # Fetches the latest data from the data.sensor.community API
    url = "https://data.sensor.community/static/v2/data.json"
    # Use a session to avoid creating a new connection for each request
    session = Session()
    try:
        print(
            dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            + " 1. Fetching the latest data..."
        )
        response = session.get(url)
        # If the response was successful, no Exception will be raised
        if response.status_code == 200 and response.content:
            # Convert the response to a Spark DataFrame
            df = sparkSession.read.option("multiline", "true").json(
                sparkSession.sparkContext.parallelize([response.text])
            )
            print(
                dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                + " Done fetching the latest data.\n"
            )
            return df
    except Exception as e:
        print(f"Request failed with exception {e}")
    finally:
        session.close()
    return None
