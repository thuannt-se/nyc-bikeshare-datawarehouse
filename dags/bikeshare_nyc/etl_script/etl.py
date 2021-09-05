from pyspark.sql import SparkSession
import os
import argparse
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType
from pyspark.sql.functions import to_timestamp, monotonically_increasing_id, col, year, month, dayofmonth, hour,\
                                    weekofyear, dayofweek, date_format, quarter

def generate_series(spark, start, stop, interval):
    """
    :param spark  - input spark session
    :param start  - lower bound, inclusive
    :param stop   - upper bound, exclusive
    :interval int - increment interval in seconds
    """
    # Determine start and stops in epoch seconds
    start, stop = spark.createDataFrame(
        [(start, stop)], ("start", "stop")
    ).select(
        [col(c).cast("timestamp").cast("long") for c in ("start", "stop")
    ]).first()
    # Create range with increments and cast to timestamp
    return spark.range(start, stop, interval).select(
        col("id").cast("timestamp").alias("Date")
    )

def create_spark_session():
    spark = SparkSession \
        .builder \
        .master("yarn") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .appName("CityBike-ETL") \
        .getOrCreate()
    return spark

def create_dim_station_df(spark: SparkSession, data):
    dim_station_schema = StructType([ \
        StructField("station_id", IntegerType(), False), \
        StructField("name", StringType(), False), \
        StructField("longitude", DoubleType(), False), \
        StructField("latitude", DoubleType(), False)
    ])
    return spark.createDataFrame(data=data, schema=dim_station_schema)

def process_citibike_tripdata(spark_session, input_path, output_path, patterns):
    """
      Extract citibike data and transform them into csv files
      Keywork argument:
      spark -- key to return udf to get correct date
      input_data -- s3 uri to get the data
      output_data -- s3 uri to store result files
    """
    final_dim_station_df = create_dim_station_df(spark_session, data=[])
    for pattern in patterns:
        citibike_data = os.path.join(input_path, "citibike-tripdata/" + pattern + "*.csv")

        df = spark_session.read.csv(citibike_data, sep=",", inferSchema=True, header=True)
        # I subtract some record that have start station same as end station and with tripduration too short (under 300s)
        filtered_df = df.subtract(df.filter(df["start station id"] == df["end station id"]).filter(df["tripduration"] < 300))


        start_station_data = filtered_df.select(col("start station id").alias("station_id"),
                                                col("start station name").alias("name"), \
                                                col("start station longitude").alias("longitude"),
                                                col("start station latitude").alias("latitude")).where(
            col("bikeid").isNotNull()).dropDuplicates().collect()

        start_station_df = create_dim_station_df(spark_session, data=start_station_data)

        end_station_data = filtered_df.select(col("end station id").alias("station_id"),
                                              col("end station name").alias("name"), \
                                              col("end station longitude").alias("longitude"),
                                              col("end station latitude").alias("latitude")).where(
                                              col("bikeid").isNotNull()).dropDuplicates().collect()
        end_station_df = create_dim_station_df(spark_session, data=end_station_data)

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
            .withColumn("start_time", date_format(to_timestamp(col("starttime")), "yyyy-MM-dd HH:mm:ss").cast('timestamp')) \
            .withColumn("end_time", date_format(to_timestamp(col("stoptime")), "yyyy-MM-dd HH:mm:ss").cast('timestamp')) \
            .select(col("trip_id"), col("tripduration").alias("duration"), col("start_time"), col("end_time"),
                    col("start station id").alias("start_station_id"),
                    col("end station id").alias("end_station_id"), col("bikeid"), col("usertype"), col("gender"),
                    col("birth year").alias("birth_year").cast("int")).collect()

        trip_fact_df = spark_session.createDataFrame(data=trip_fact_data, schema=trip_fact_schema)

        trip_fact_df.write.mode("append").option('timestampFormat', 'yyyy-MM-dd HH:mm:ss')\
            .csv(os.path.join(output_path, "tripfact-table"))
        final_dim_station_df.union(dim_station_df).dropDuplicates()

    final_dim_station_df.write.mode("append").mode("overwrite").csv(os.path.join(output_path, "dim-station-table"))

def create_weather_relation_wt_df(df, cols, spark, schema):
    emptyRDD = spark.sparkContext.emptyRDD()
    result = spark.createDataFrame(emptyRDD, schema)
    for i in cols:
        rowDict = []
        data = df.select(to_timestamp("date_time").alias("date_time"), i).collect()
        for row in data:
            if row[i] != None and row[i].strip() == "1":
                rowDict.append((row["date_time"], int(i[-2:])))
        row_df = spark.createDataFrame(rowDict, ["date_time", "weather_type_id"])
        result = result.union(row_df)
    return result;

def process_weather_data(spark_session, input_path, output_path):

    weather_file = os.path.join(input_path, "weather-data/*.csv")

    weather_df = spark_session.read.option("header", "true").csv(weather_file)

    data = weather_df.select(col("DATE").alias("date_time"), col("PRCP").alias("prcp"), col("SNOW").alias("snow"), col("SNWD").alias("snwd"),
                             col("TAVG").alias("tavg"), col("TMAX").alias("tmax"), col("TMIN").alias("tmin"),
                             "WT01", "WT02", "WT03", "WT04", "WT05", "WT06",
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
    weather_fact_df = spark_session.createDataFrame(
    weather_fact_df.select(to_timestamp(weather_fact_df['date_time']).alias('date_time'), weather_fact_df['prcp'].cast('double'), \
                               weather_fact_df['snow'].cast('double'), weather_fact_df['snwd'].cast('double'), \
                               weather_fact_df['tavg'].cast('double'), weather_fact_df['tmax'].cast('double'), \
                               weather_fact_df['tmin'].cast('double')).collect()).dropDuplicates()
    weather_fact_df.write.mode("overwrite").option('timestampFormat', 'yyyy-MM-dd HH:mm:ss').csv(os.path.join(output_path, "weather-fact-table"))
    weather_type_df.write.mode("overwrite").csv(os.path.join(output_path, "weather-type-table"))
    weather_date_relation_type_df.write.mode("overwrite").option('timestampFormat', 'yyyy-MM-dd HH:mm:ss').csv(os.path.join(output_path, "dim-datetime-weather-table"))


def process_date_time_data(spark_session, output):
    generated_date_series = generate_series(spark_session, '2020-01-1', '2020-12-31', 60 * 60)

    dim_datetime_df = generated_date_series.withColumn('hour', hour(generated_date_series.Date))\
        .withColumn('day', dayofmonth(generated_date_series.Date)) \
        .withColumn('week', weekofyear(generated_date_series.Date))\
        .withColumn('month',month(generated_date_series.Date)) \
        .withColumn('weekday', dayofweek(generated_date_series.Date))\
        .withColumn('year', year(generated_date_series.Date)) \
        .withColumn('quarter', quarter(generated_date_series.Date))
    dim_datetime_df.write.mode("overwrite").option('timestampFormat', 'yyyy-MM-dd HH:mm:ss').csv(os.path.join(output, "dim-datetime-table"))


if __name__ == "__main__":
    spark_session = create_spark_session()
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default="s3://nyc-bikeshare-trip-data/")
    parser.add_argument("--output", type=str, default="s3://thuannt.se-default-bucket/")
    args = parser.parse_args()
    process_citibike_tripdata(spark_session, args.input, args.output, ["*01-", "*02-", "*03-", "*04-", "*05-", "*06-",
                                                                       "*07-", "*08-", "*09-", "*10-", "*11-", "*12-"])
    process_weather_data(spark_session, args.input, args.output)
    process_date_time_data(spark_session, args.output)
