from pyspark.sql.types import BooleanType
import pyspark.sql.functions as F
from pyspark.sql import Row
from botocore.config import Config
import boto3
import time
import datetime


def keepOnlyUpdatedRows(database_name, table_name, df, last_timestamps):
    query = """
        SELECT sensor_id, MAX(time) as last_timestamp
        FROM %s.%s
        GROUP BY sensor_id 
    """ % (
        database_name,
        table_name,
    )

    # Exécution de la requête
    response = boto3.client("timestream-query").query(QueryString=query)

    # Traitement des résultats
    last_timestamps = {}
    for row in response["Rows"]:
        sensor_id = row["Data"][0]["ScalarValue"]
        last_timestamp = row["Data"][1]["ScalarValue"]
        last_timestamps[sensor_id] = last_timestamp

    @F.udf(returnType=BooleanType())
    def isUpdated(sensor_id, timestamp, last_timestamps):
        if str(sensor_id) not in last_timestamps:
            return True
        current_timestamp = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        last_timestamp_micro = last_timestamps[str(sensor_id)][
            :26
        ]  # Keep only up to microseconds
        last_sensor_timestamp = datetime.datetime.strptime(
            last_timestamp_micro, "%Y-%m-%d %H:%M:%S.%f"
        )
        return current_timestamp > last_sensor_timestamp

    # Filtrer le DataFrame pour inclure seulement les données mises à jour
    df_updated = df.filter(isUpdated(F.col("sensor_id"), F.col("timestamp")))
    return df_updated


# Write records to Timestream
def write_records(database_name, table_name, client, records, common_attributes):
    try:
        client.write_records(
            DatabaseName=database_name,
            TableName=table_name,
            CommonAttributes=common_attributes,
            Records=records,
        )
    except client.exceptions.RejectedRecordsException as e:
        print("Rejected Records:", e.response)
    except Exception as e:
        print("Error:", e)
        raise


def write_partition_to_timestream(partition):
    # Initialize the boto3 client for each partition
    write_client = boto3.client(
        "timestream-write",
        config=Config(
            read_timeout=20, max_pool_connections=5000, retries={"max_attempts": 10}
        ),
    )

    # Create a list of records
    records = []
    for row in partition:
        try:
            # Skip rows that are not of type Row
            if not isinstance(row, Row):
                continue

            # Convert timestamp to Unix epoch time in milliseconds
            timestamp_datetime = datetime.datetime.strptime(
                row.timestamp, "%Y-%m-%d %H:%M:%S"
            )
            row_timestamp = str(int(timestamp_datetime.timestamp() * 1000))

            altitude = row.altitude if row.altitude != "" else "0"

            valueId = (
                "aqi_%s" % row.sensor_id
                if row.aqi is not None and row.value_type == "P1"
                else str(row.sensordatavalues_id)
            )

            # Create dimensions list
            dimensions = [
                {"Name": "country", "Value": str(row.country)},
                {"Name": "latitude", "Value": str(row.latitude)},
                {"Name": "longitude", "Value": str(row.longitude)},
                {"Name": "altitude", "Value": altitude},
                {"Name": "location_id", "Value": str(row.location_id)},
                {"Name": "sensor_id", "Value": str(row.sensor_id)},
                {"Name": "value_id", "Value": valueId},
                {
                    "Name": "sensor_type_manufacturer",
                    "Value": str(row.sensor_type_manufacturer),
                },
                {"Name": "sensor_type_name", "Value": str(row.sensor_type_name)},
            ]

            # Create common attributes dictionary
            common_attributes = {"Dimensions": dimensions, "Time": row_timestamp}

            # Create a record for each measurement
            record = {
                "MeasureName": str(row.value_type),
                "MeasureValue": str(row.value),
                "MeasureValueType": "DOUBLE",
            }
            records.append(record)
            if len(records) == 100:
                write_records(write_client, records, common_attributes)
                records = []
                time.sleep(1)

            # Check and add AQI record if present
            if row.aqi is not None and row.value_type == "P1":
                aqi_record = {
                    "MeasureName": "aqi",
                    "MeasureValue": str(row.aqi),
                    "MeasureValueType": "BIGINT",
                }
                records.append(aqi_record)
                if len(records) == 100:
                    write_records(write_client, records, common_attributes)
                    records = []
                    time.sleep(1)

        except Exception as e:
            print(f"Error processing row: {row}")
            print(f"Exception: {e}")

    # Write records to Timestream
    if len(records) > 100:
        while len(records) > 100:
            write_records(write_client, records[:99], common_attributes)
            records = records[99:]  # Keep the remaining records
            time.sleep(1)
    else:
        write_records(write_client, records, common_attributes)
