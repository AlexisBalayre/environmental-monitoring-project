from pyspark.sql.types import FloatType, IntegerType
import pyspark.sql.functions as F


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
    df_aqi = df.withColumn(
        "sensordatavalues",
        F.explode("sensordatavalues"),  # Explode the array to separates values
    ).withColumn(
        "aqi",
        F.when(
            F.col("sensordatavalues.value_type") == "P1",
            get_aqi_value_p25(
                F.col("sensordatavalues.value").cast(FloatType())
            ),  # Cast the value to float and compute the AQI of PM2.5
        ).when(
            F.col("sensordatavalues.value_type") == "P2",
            get_aqi_value_p10(
                F.col("sensordatavalues.value").cast(FloatType())
            ),  # Cast the value to float and compute the AQI of PM10
        ),
    )

    df_final = df_aqi.groupBy("id").agg(
        F.max("aqi").alias("aqi"),
    )

    # Join the original dataframe with the AQI values
    df_joined = df.join(df_final, "id", "left")

    return df_joined





""" # Flatten the dataframe
df_flattened = (
    df_joined.select(
        "id",
        "timestamp",
        "location",
        "sensor",
        "sensordatavalues",
        "aqi",
    )
    .withColumn(
        "sensordatavalues",
        F.explode("sensordatavalues"),  # Explode the array to separates values
    )
    .selectExpr(
        "id",
        "timestamp",
        "location.country as country",
        "location.latitude as latitude",
        "location.longitude as longitude",
        "location.altitude as altitude",
        "location.id as location_id",
        "sensor.id as sensor_id",
        "sensor.pin as sensor_pin",
        "sensor.sensor_type.id as sensor_type_id",
        "sensor.sensor_type.manufacturer as sensor_type_manufacturer",
        "sensor.sensor_type.name as sensor_type_name",
        "sensordatavalues.value_type as value_type",
        "sensordatavalues.value as value",
        "sensordatavalues.id as sensordatavalues_id",
        "aqi",
    )
) """