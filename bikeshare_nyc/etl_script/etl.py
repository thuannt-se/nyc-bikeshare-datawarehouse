from pyspark.sql import SparkSession
import os
import argparse
import io
import pandas as pd
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, LongType, TimestampType
from pyspark.sql.functions import to_timestamp, monotonically_increasing_id, udf, col, year, month, dayofmonth, hour, weekofyear, date_format, date_trunc, when

def create_date_table(start='2019-01-01', end='2019-12-31'):
    df = pd.DataFrame({"Date": pd.date_range(start, end, freq = "H")})
    df['hour'] = df.Date.dt.hour
    df['day'] = df.Date.dt.day
    df["week"] = df.Date.dt.weekofyear
    df['month'] = df.Date.dt.month
    df["weekday"] = df.Date.dt.weekday
    df["quarter"] = df.Date.dt.quarter
    df["year"] = df.Date.dt.year
    return df


def create_spark_session():
    spark = SparkSession \
        .builder \
        .master("yarn") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .appName("CityBike-ETL") \
        .getOrCreate()
    return spark

def process_citibike_tripdata(spark_session, input_path, output_path):
    """
      Extract citibike data and transform them into parquet files
      Keywork argument:
      spark -- key to return udf to get correct date
      input_data -- s3 uri to get the data
      output_data -- s3 uri to store result files
    """
    citibike_data = os.path.join(input_path, "citibike-tripdata/*.csv")

    df = spark_session.read.csv(citibike_data, sep=",", inferSchema=True, header=True)
    # I subtract some record that have start station same as end station and with tripduration too short (under 300s)
    filtered_df = df.subtract(df.filter(df["start station id"] == df["end station id"]).filter(df["tripduration"] < 300))
    dim_time_schema = StructType([ \
        StructField("Date", TimestampType(), True), \
        StructField("hour", IntegerType(), True), \
        StructField("day", IntegerType(), True), \
        StructField("week", IntegerType(), True), \
        StructField("month", IntegerType(), True), \
        StructField("weekday", IntegerType(), True), \
        StructField("quarter", IntegerType(), True), \
        StructField("year", IntegerType(), True)
    ])

    dim_datetime_df = spark_session.createDataFrame(create_date_table(), schema=dim_time_schema)

    dim_station_schema = StructType([ \
        StructField("station_id", IntegerType(), False), \
        StructField("name", StringType(), False), \
        StructField("longitude", DoubleType(), False), \
        StructField("latitude", DoubleType(), False)
    ])

    start_station_data = filtered_df.select(col("start station id").alias("station_id"),
                                            col("start station name").alias("name"), \
                                            col("start station longitude").alias("longitude"),
                                            col("start station latitude").alias("latitude")).where(
        col("bikeid").isNotNull()).dropDuplicates().collect()

    start_station_df = spark_session.createDataFrame(data=start_station_data, schema=dim_station_schema)

    end_station_data = filtered_df.select(col("end station id").alias("station_id"),
                                          col("end station name").alias("name"), \
                                          col("end station longitude").alias("longitude"),
                                          col("end station latitude").alias("latitude")).where(
        col("bikeid").isNotNull()).dropDuplicates().collect()
    end_station_df = spark_session.createDataFrame(data=end_station_data, schema=dim_station_schema)

    dim_station_df = start_station_df.union(end_station_df).dropDuplicates()

    trip_fact_schema = StructType([ \
        StructField("trip_id", LongType(), False), \
        StructField("duration", IntegerType(), False), \
        StructField("start_time", TimestampType(), False), \
        StructField("end_time", TimestampType(), False), \
        StructField("start_station_id", IntegerType(), False), \
        StructField("end_station_id", IntegerType(), False), \
        StructField("bikeid", IntegerType(), False), \
        StructField("usertype", StringType(), False), \
        StructField("gender", IntegerType(), False), \
        StructField("birth_year", IntegerType(), True)
    ])

    trip_fact_data = filtered_df.withColumn("trip_id", monotonically_increasing_id()) \
        .withColumn("start_time", date_trunc("hour", to_timestamp(col("starttime")))) \
        .withColumn("end_time", date_trunc("hour", to_timestamp(col("stoptime")))) \
        .select(col("trip_id"), col("tripduration").alias("duration"), col("start_time"), col("end_time"),
                col("start station id").alias("start_station_id"),
                col("end station id").alias("end_station_id"), col("bikeid"), col("usertype"), col("gender"),
                col("birth year").alias("birth_year").cast("int")).collect()

    trip_fact_df = spark_session.createDataFrame(data=trip_fact_data, schema=trip_fact_schema)

    trip_fact_df.withColumn("year", year(col("start_time"))).withColumn("month", month(col("start_time")))\
        .write.partitionBy("year", "month").mode("overwrite")\
        .parquet(os.path.join(output_path, "tripfact-table"))
    dim_station_df.write.mode("overwrite").parquet(os.path.join(output_path, "dim-station-table"))
    dim_datetime_df.write.mode("overwrite").parquet(os.path.join(output_path, "dim-datetime-table"))


def create_weather_relation_wt_df(df, cols, spark, schema):
    emptyRDD = spark.sparkContext.emptyRDD()
    result = spark.createDataFrame(emptyRDD, schema)
    for i in cols:
        rowDict = []
        data = df.select(to_timestamp("DATE").alias("date_time"), i).collect()
        for row in data:
            if row[i] != None and row[i].strip() == "1":
                rowDict.append((row["date_time"], int(i[-2:])))
        row_df = spark.createDataFrame(rowDict, ["date_time", "weather_type_id"])
        result = result.union(row_df)
    return result;

def process_weather_data(spark_session, input_path, output_path):

    weather_file = os.path.join(input_path, "weather-data/*.csv")

    weather_df = spark_session.read.option("header", "true").csv(weather_file)

    data = weather_df.select("DATE", "PRCP", "SNOW", "SNWD", "TAVG",
                             "TMAX", "TMIN", "WT01", "WT02", "WT03", "WT04", "WT05", "WT06",
                             "WT08", "WT09", "WT11").collect()

    weather_fact_df = spark_session.createDataFrame(data)

    date_with_weather_type_schema = StructType([ \
        StructField("date_time", TimestampType(), False), \
        StructField("weather_type_id", IntegerType(), False)
    ])
    weather_type_schema = StructType([ \
        StructField("weather_type_id", IntegerType(), False), \
        StructField("description", StringType(), False)
    ])

    weather_type_data = [(1, "Fog, ice fog, or freezing fog (may include heavy fog)"), \
                         (2, "Heavy fog or heaving freezing fog (not always distinquished from fog)"), \
                         (3, "Thunder"), \
                         (4, "Ice pellets, sleet, snow pellets, or small hail"), \
                         (5, "Hail (may include small hail)"), \
                         (6, "Glaze or rime"), \
                         (7, "Dust, volcanic ash, blowing dust, blowing sand, or blowing obstruction"), \
                         (8, "Smoke or haze"), \
                         (9, "Blowing or drifting snow"), \
                         (10, "Tornado, waterspout, or funnel cloud"), \
                         (11, "High or damaging winds"), \
                         (12, "Blowing spray"), \
                         (13, "Mist"), \
                         (14, "Drizzle"), \
                         (15, "Freezing drizzle"), \
                         (16, "Rain (may include freezing rain, drizzle, and freezing drizzle) "), \
                         (17, "Freezing rain"), \
                         (18, "Snow, snow pellets, snow grains, or ice crystals"), \
                         (19, "Unknown source of precipitation"), \
                         (21, "Ground fog"), \
                         (22, "Ice fog or freezing fog")
                         ]
    weather_type_df = spark_session.createDataFrame(data=weather_type_data, schema=weather_type_schema)
    weather_date_relation_type_df = create_weather_relation_wt_df(weather_fact_df,
                                                                  ["WT01", "WT02", "WT03", "WT04", "WT05", "WT06",
                                                                   "WT08", "WT09", "WT11"],
                                                                  spark_session, date_with_weather_type_schema).dropDuplicates()
    weather_fact_df = weather_fact_df.drop(weather_fact_df.WT01).drop(weather_fact_df.WT02).drop(
        weather_fact_df.WT03).drop(weather_fact_df.WT04) \
        .drop(weather_fact_df.WT05).drop(weather_fact_df.WT06).drop(weather_fact_df.WT08).drop(
        weather_fact_df.WT09).drop(weather_fact_df.WT11)

    weather_fact_df.write.mode("overwrite").parquet(os.path.join(output_path, "weather-fact-table"))
    weather_type_df.write.mode("overwrite").parquet(os.path.join(output_path, "weather-type-table"))
    weather_date_relation_type_df.write.mode("overwrite").parquet(os.path.join(output_path, "dim-datetime-table"))


if __name__ == "__main__":
    spark_session = create_spark_session()
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default="s3://nyc-bikeshare-trip-data/")
    parser.add_argument("--output", type=str, default="s3://thuannt.se-default-bucket/")
    args = parser.parse_args()
    process_citibike_tripdata(spark_session, args.input, args.output)
    process_weather_data(spark_session, args.input, args.output)
