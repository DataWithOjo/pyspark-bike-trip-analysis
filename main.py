from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import zipfile
import os

DATA_DIR = "data"
REPORTS_DIR = "reports"


def unzip_data():
    for file in os.listdir(DATA_DIR):
        if file.endswith(".zip"):
            with zipfile.ZipFile(os.path.join(DATA_DIR, file), "r") as zip_ref:
                zip_ref.extractall(DATA_DIR)


def load_trip_data(spark):
    df = spark.read.option("header", True).csv(f"{DATA_DIR}/*.csv", inferSchema=True)
    df = (
        df.withColumn("start_time", to_timestamp("start_time"))
        .withColumn("end_time", to_timestamp("end_time"))
        .withColumn("trip_date", to_date("start_time"))
        .withColumn("birthyear", col("birthyear").cast("int"))
        .withColumn("age", year(col("start_time")) - col("birthyear"))
    )
    return df


def average_trip_duration_per_day(df):
    print("Running: average_trip_duration_per_day")
    result = df.groupBy("trip_date").agg(avg("tripduration").alias("avg_trip_duration"))
    result.write.csv(
        f"{REPORTS_DIR}/average_trip_duration_per_day", header=True, mode="overwrite"
    )


def trips_per_day(df):
    print("Running: trips_per_day")
    result = df.groupBy("trip_date").count().withColumnRenamed("count", "trip_count")
    result.write.csv(f"{REPORTS_DIR}/trips_per_day", header=True, mode="overwrite")


def most_popular_start_station_per_month(df):
    print("Running: most_popular_start_station_per_month")
    df = df.withColumn("month", date_format("start_time", "yyyy-MM"))
    grouped = df.groupBy("month", "from_station_name").count()
    windowed = Window.partitionBy("month").orderBy(desc("count"))
    result = (
        grouped.withColumn("rank", row_number().over(windowed))
        .filter(col("rank") == 1)
        .drop("rank")
    )
    result.write.csv(
        f"{REPORTS_DIR}/most_popular_start_station_per_month",
        header=True,
        mode="overwrite",
    )


def top_3_trip_stations_last_2_weeks(df):
    print("Running: top_3_trip_stations_last_2_weeks")
    max_date = df.agg(max("trip_date")).first()[0]
    recent_df = df.filter(col("trip_date") >= date_sub(lit(max_date), 13))
    grouped = recent_df.groupBy("trip_date", "from_station_name").count()
    windowed = Window.partitionBy("trip_date").orderBy(desc("count"))
    result = (
        grouped.withColumn("rank", row_number().over(windowed))
        .filter(col("rank") <= 3)
        .drop("rank")
    )
    result.write.csv(
        f"{REPORTS_DIR}/top_3_trip_stations_last_2_weeks", header=True, mode="overwrite"
    )


def average_trip_by_gender(df):
    print("Running: average_trip_duration_by_gender")
    result = df.groupBy("gender").agg(avg("tripduration").alias("avg_trip_duration"))
    result.write.csv(
        f"{REPORTS_DIR}/average_trip_by_gender", header=True, mode="overwrite"
    )


def top10_ages_longest_shortest(df):
    print("Running: top_10_ages_by_trip_duration")
    longest = (
        df.orderBy(desc("tripduration"))
        .select("age", "tripduration")
        .na.drop()
        .limit(10)
    )
    shortest = (
        df.orderBy("tripduration").select("age", "tripduration").na.drop().limit(10)
    )
    longest.write.csv(
        f"{REPORTS_DIR}/top10_ages_longest_trips", header=True, mode="overwrite"
    )
    shortest.write.csv(
        f"{REPORTS_DIR}/top10_ages_shortest_trips", header=True, mode="overwrite"
    )


def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()

    unzip_data()

    print("Reading and processing data...")
    df = load_trip_data(spark)

    try:
        average_trip_duration_per_day(df)
        trips_per_day(df)
        most_popular_start_station_per_month(df)
        top_3_trip_stations_last_2_weeks(df)
        average_trip_by_gender(df)
        top10_ages_longest_shortest(df)
    except Exception as e:
        print("Error while generating reports:", e)

    print("All reports written to the 'reports/' folder.")
    spark.stop()


if __name__ == "__main__":
    main()
