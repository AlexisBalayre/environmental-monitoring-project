import findspark

findspark.init()  # Initializing Spark

from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, IntegerType, BooleanType
from pyspark.sql import Row
import pyspark.sql.functions as F

import boto3
from botocore.config import Config

from requests import Session

import datetime as dt
import time

# Initializing Spark Session
sparkSession = (
    SparkSession.builder.appName("Cloud Computing Project")
    .master("local[*]")
    .config("spark.sql.inMemoryColumnarStorage.compressed", "true")
    .config("spark.sql.inMemoryColumnarStorage.batchSize", "10000")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.ui.enabled", "true")
    .config("spark.io.compression.codec", "snappy")
    .config("spark.rdd.compress", "true")
    .getOrCreate()
)


def fetch_sensors_data():
    """
    Fetches the latest data from the sensors and writes it to a CSV file

    Returns:
        now (datetime): The timestamp of the fetched data
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


# Defining a UDF to compute the AQI value for PM2.5
@F.udf(returnType=IntegerType())
def get_aqi_value_p25(value):
    if value is None:
        return None
    if 0 <= value <= 11:
        return 1
    elif 12 <= value <= 23:
        return 2
    elif 24 <= value <= 35:
        return 3
    elif 36 <= value <= 41:
        return 4
    elif 42 <= value <= 47:
        return 5
    elif 48 <= value <= 53:
        return 6
    elif 54 <= value <= 58:
        return 7
    elif 59 <= value <= 64:
        return 8
    elif 65 <= value <= 70:
        return 9
    return 10


# Defining a UDF to compute the AQI value for PM10
@F.udf(returnType=IntegerType())
def get_aqi_value_p10(value):
    if value is None:
        return None
    if 0 <= value <= 16:
        return 1
    elif 17 <= value <= 33:
        return 2
    elif 34 <= value <= 50:
        return 3
    elif 51 <= value <= 58:
        return 4
    elif 59 <= value <= 66:
        return 5
    elif 67 <= value <= 75:
        return 6
    elif 76 <= value <= 83:
        return 7
    elif 84 <= value <= 91:
        return 8
    elif 92 <= value <= 99:
        return 9
    return 10


def computeAQI(df):
    print(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " 2. Computing the AQI...")
    df_exploded = df.withColumn(
        "sensordatavalue", F.explode("sensordatavalues")
    ).withColumn(
        "aqi",
        F.when(
            F.col("sensordatavalue.value_type") == "P1",
            get_aqi_value_p25(
                F.col("sensordatavalue.value").cast(FloatType())
            ),  # Cast the value to float and compute the AQI of PM2.5
        ).when(
            F.col("sensordatavalue.value_type") == "P2",
            get_aqi_value_p10(
                F.col("sensordatavalue.value").cast(FloatType())
            ),  # Cast the value to float and compute the AQI of PM10
        ),
    )
    df_exploded.cache()
    df_grouped = (
        df_exploded.groupBy("sensor.id", "timestamp")
        .agg(
            F.first("id").alias("id"),
            F.first("location").alias("location"),
            F.first("sensor").alias("sensor"),
            F.max("aqi").alias("aqi"),
            F.collect_list("sensordatavalue").alias("sensordatavalues"),
        )
        .selectExpr(
            "sensor.id as sensor_id",
            "sensor.pin as sensor_pin",
            "sensor.sensor_type.id as sensor_type_id",
            "sensor.sensor_type.manufacturer as sensor_type_manufacturer",
            "sensor.sensor_type.name as sensor_type_name",
            "location.country as country",
            "location.latitude as latitude",
            "location.longitude as longitude",
            "location.altitude as altitude",
            "location.id as location_id",
            "aqi",
            "sensordatavalues",
            "timestamp",
        )
    )
    df_exploded.unpersist()
    print(
        dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " Done computing the AQI.\n"
    )
    return df_grouped


def keepOnlyUpdatedRows(database_name, table_name, df):
    print(
        dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        + " 3. Filtering the data to keep only the updated rows..."
    )
    query = """
        SELECT sensor_id, MAX(time) as last_timestamp
        FROM {}.{}
        GROUP BY sensor_id 
    """.format(
        database_name, table_name
    )

    # Exécution de la requête
    session = boto3.Session()
    query_client = session.client(
        "timestream-query", config=Config(region_name="us-east-1")
    )
    paginator = query_client.get_paginator("query")

    last_timestamps = {}
    response_iterator = paginator.paginate(QueryString=query)
    for response in response_iterator:
        for row in response["Rows"]:
            sensor_id = row["Data"][0]["ScalarValue"]
            last_timestamp = row["Data"][1]["ScalarValue"]
            last_timestamps[sensor_id] = last_timestamp

    if len(last_timestamps) == 0:
        print("No data in Timestream")
        return df

    @F.udf(returnType=BooleanType())
    def isUpdated(sensor_id, timestamp):
        if str(sensor_id) not in last_timestamps:
            return True
        current_timestamp = dt.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        last_timestamp_micro = last_timestamps[str(sensor_id)][
            :26
        ]  # Keep only up to microseconds
        last_sensor_timestamp = dt.datetime.strptime(
            last_timestamp_micro, "%Y-%m-%d %H:%M:%S.%f"
        )
        return current_timestamp > last_sensor_timestamp

    # Filtrer le DataFrame pour inclure seulement les données mises à jour
    df_updated = df.filter(isUpdated("sensor_id", "timestamp"))
    print(
        dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        + " Done filtering the data to keep only the updated rows.\n"
    )
    return df_updated


def _print_rejected_records_exceptions(err):
    print("RejectedRecords: ", err)
    for rr in err.response["RejectedRecords"]:
        print("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
        if "ExistingVersion" in rr:
            print("Rejected record existing version: ", rr["ExistingVersion"])


# Write records to Timestream
def write_records(database_name, table_name, client, records):
    try:
        result = client.write_records(
            DatabaseName=database_name,
            TableName=table_name,
            CommonAttributes={},
            Records=records,
        )
        print(
            "WriteRecords Status: [%s]" % result["ResponseMetadata"]["HTTPStatusCode"]
        )
    except client.exceptions.RejectedRecordsException as err:
        _print_rejected_records_exceptions(err)
    except Exception as err:
        print("Error:", err)


def writeToTimestream(database_name, table_name, partionned_df):
    # Initialize the boto3 client for each partition
    session = boto3.Session()
    write_client = session.client(
        "timestream-write",
        config=Config(
            read_timeout=20, max_pool_connections=5000, retries={"max_attempts": 10}
        ),
    )

    # Create a list of records
    records = []
    for row in partionned_df:
        try:
            # Skip rows that are not of type Row
            if not isinstance(row, Row):
                continue

            # Convert timestamp to Unix epoch time in milliseconds
            timestamp_datetime = dt.datetime.strptime(
                row.timestamp, "%Y-%m-%d %H:%M:%S"
            )
            row_timestamp = str(int(timestamp_datetime.timestamp() * 1000))

            # altitude
            altitude = row.altitude if row.altitude != "" else 0

            # Create dimensions list
            dimensions = [
                {"Name": "country", "Value": str(row.country)},
                {"Name": "latitude", "Value": str(row.latitude)},
                {"Name": "longitude", "Value": str(row.longitude)},
                {"Name": "altitude", "Value": str(altitude)},
                {"Name": "location_id", "Value": str(row.location_id)},
                {"Name": "sensor_id", "Value": str(row.sensor_id)},
                {"Name": "sensor_pin", "Value": str(row.sensor_pin)},
                {
                    "Name": "sensor_type_manufacturer",
                    "Value": str(row.sensor_type_manufacturer),
                },
                {"Name": "sensor_type_name", "Value": str(row.sensor_type_name)},
                {"Name": "sensor_type_id", "Value": str(row.sensor_type_id)},
            ]

            # Create a record for each measurement
            measuresValues = []
            for measure in row.sensordatavalues:
                measureValue = {
                    "Name": measure.value_type,
                    "Value": str(measure.value),
                    "Type": "DOUBLE",
                }
                measuresValues.append(measureValue)

                if measure.value_type == "P2" and row.aqi is not None:
                    aqi_measureValue = {
                        "Name": "aqi",
                        "Value": str(row.aqi),
                        "Type": "BIGINT",
                    }
                    measuresValues.append(aqi_measureValue)

            # Create a record for each sensor
            record = {
                "Dimensions": dimensions,
                "Time": row_timestamp,
                "TimeUnit": "MILLISECONDS",
                "MeasureName": "air_quality",
                "MeasureValueType": "MULTI",
                "MeasureValues": measuresValues,
            }
            records.append(record)
            if len(records) >= 98:
                write_records(database_name, table_name, write_client, records)
                records = []
                time.sleep(1)

        except Exception as e:
            print(f"Error processing row: {row}")
            print(f"Exception: {e}")

    # Write records to Timestream
    if len(records) > 100:
        while len(records) > 100:
            write_records(database_name, table_name, write_client, records[:99])
            records = records[99:]  # Keep the remaining records
            time.sleep(1)
    elif len(records) > 0:
        write_records(database_name, table_name, write_client, records)
