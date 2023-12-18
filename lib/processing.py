# collecting.py
# The second step of the pipeline

from pyspark.sql.types import FloatType, IntegerType
import pyspark.sql.functions as F
import datetime as dt


# Defining a UDF to compute the AQI value for PM2.5
@F.udf(returnType=IntegerType())
def get_aqi_value_p25(value):
    """
    Computes the AQI value for PM2.5

    Args:
        value (float): The value of PM2.5
    Returns:
        aqi (int): The AQI value
    """

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
    """
    Computes the AQI value for PM10

    Args:
        value (float): The value of PM10

    Returns:
        aqi (int): The AQI value
    """

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
    """
    Computes the AQI for each particulate matter sensor

    Args:
        df (DataFrame): The DataFrame containing the data from the sensors

    Returns:
        df_grouped (DataFrame): The DataFrame containing the AQI for each sensor
    """

    print(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " 2. Computing the AQI...")
    df_exploded = df.withColumn(
        "sensordatavalue",
        F.explode("sensordatavalues"),  # Explode the sensordatavalues column
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
    df_exploded.cache()  # Cache the DataFrame to avoid recomputing it
    df_grouped = (
        df_exploded.groupBy("sensor.id", "timestamp")  # Group by sensor and timestamp
        .agg(
            F.first("id").alias("id"),
            F.first("location").alias("location"),
            F.first("sensor").alias("sensor"),
            F.max("aqi").alias("aqi"),  # Compute the maximum AQI between PM2.5 and PM10
            F.collect_list("sensordatavalue").alias("sensordatavalues"),
        )  # Aggregate the AQI and the sensordatavalues
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
        )  # Select the columns to keep
    )
    df_exploded.unpersist()  # Unpersist the DataFrame to free memory
    print(
        dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " Done computing the AQI.\n"
    )
    return df_grouped
