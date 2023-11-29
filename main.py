import findspark

findspark.init()  # Initializing Spark

from pyspark.sql import SparkSession
import os

from lib.formatting import computeAQI


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

# Reading the last JSON file
current_directory = os.getcwd()
sensor_data_dir = os.path.join(current_directory, "data/sensors_data")
latest_file = sorted(os.listdir(sensor_data_dir))[-1]
path = os.path.join(sensor_data_dir, latest_file)
df = sparkSession.read.option("multiline", "true").json(path)

# Computing the AQI
df = computeAQI(df)
df.show()

# Define the Timestream database and table names
DATABASE_NAME = "iot_project"
TABLE_NAME = "iot_table_v2"