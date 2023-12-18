import findspark

findspark.init()  # Initializing Spark

from pyspark.sql import SparkSession

import datetime as dt
import time

from lib.collecting import fetch_sensors_data
from lib.processing import computeAQI
from lib.storing import keepOnlyUpdatedRows, writeToTimestream

if __name__ == "__main__":
    # Define the Timestream database and table names
    DATABASE_NAME = "iot_project"
    TABLE_NAME = "iot_table"

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

    while True:
        try:
            print(
                dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                + " Starting the pipeline..."
            )
            # Fetch the data from the sensors
            iotDfRaw = fetch_sensors_data(sparkSession)

            # Compute the AQI for each sensor
            iotDfFormatted = computeAQI(iotDfRaw)

            # Filter the data to keep only the updated rows
            dataFiltered = keepOnlyUpdatedRows(
                DATABASE_NAME, TABLE_NAME, iotDfFormatted
            )

            # Write the data to Timestream
            print(
                dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                + " 4. Writing the data to Timestream..."
            ) 
            dataFiltered.foreachPartition(
                lambda partition: writeToTimestream(
                    DATABASE_NAME, TABLE_NAME, partition
                ) 
            ) 
            print(
                dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                + " Done writing the data to Timestream.\n"
            )

            # Sleep for 10 seconds
            print(
                dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                + " Done with the pipeline. Waiting for 2 minutes.\n"
            )
            time.sleep(10)
        except Exception as e:
            print(f"Exception: {e}")
