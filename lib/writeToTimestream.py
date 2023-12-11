from pyspark.sql.types import BooleanType
import pyspark.sql.functions as F
from pyspark.sql import Row
from botocore.config import Config
import boto3
import time
import datetime as dt


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
