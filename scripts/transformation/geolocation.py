import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import get_spark_session, s3_path, write_silver_csv
from pyspark.sql.functions import col, trim, lower


def run():
    spark = get_spark_session("bronze-to-silver-geolocation")

    geolocation = spark.read.csv(
        s3_path("bronze", "geolocation", "olist_geolocation_dataset.csv"),
        header=True, inferSchema=True,
    )

    geolocation_clean = geolocation.dropDuplicates()
    geolocation_clean = geolocation_clean.withColumn(
        "geolocation_city", lower(trim(col("geolocation_city")))
    )

    write_silver_csv(geolocation_clean, "geolocation", "geolocation.csv")
    spark.stop()


if __name__ == "__main__":
    run()
