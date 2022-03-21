"""
This is Test Task for MobiLab

The app will:
1) Parse NYC TLC website for datasets
2) Download at least last 3 years of data
2.1) Bypass - to download only 1 small file
3) Reformat CSV files into Avro and/or Parquet format
4) Queries Data for:
4.1) Average Trip Distance
4.2) Busiest Hours
4.3) Day of week with the lowest number of single rider trips
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from app import scrapper, converter


def queries() -> None:
    """
    Quering Examples Function
    """
    # Quering example
    spark = SparkSession.builder.appName("Application name").enableHiveSupport().getOrCreate()
    data = spark.read.parquet("./records/2021/green_tripdata_2021-01.parquet")

    # average trip distance
    data.select("trip_distance").agg(avg(col("trip_distance"))).show()

    # Busiest hours
    data.createOrReplaceTempView("parquetFile")
    spark.sql(
        "SELECT hour(timestamp(lpep_pickup_datetime)) AS hour,"
        " COUNT(*) as occurance FROM parquetFile"
        " GROUP BY hour ORDER BY occurance DESC LIMIT 3"
    ).show()

    # Day of week with the lowest number of single rider trips
    spark.sql(
        "SELECT dayofweek(timestamp(lpep_pickup_datetime)) AS day,"
        " COUNT(*) as occurance FROM parquetFile"
        " WHERE passenger_count = 1 GROUP BY day"
        " ORDER BY occurance ASC LIMIT 1"
    ).show()


def app() -> None:
    """
    Application`s main entry point
    """
    print(__doc__)
    print("Running application\n")

    # Data pipeline
    # Bypass - to download only 1 smallest file for example
    scrapper.run(bypass=True)
    converter.run()
    queries()
    print("\nExiting application")


if __name__ == "__main__":
    app()
