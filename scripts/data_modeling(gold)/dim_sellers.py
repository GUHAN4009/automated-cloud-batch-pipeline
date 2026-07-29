import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import get_spark_session, s3_path, write_gold_csv
from pyspark.sql import Window
from pyspark.sql.functions import row_number


def run():
    spark = get_spark_session("silver-to-gold-dim-sellers")

    sellers = spark.read.csv(
        s3_path("silver", "sellers", "sellers.csv"),
        header=True, inferSchema=True,
    )

    dim_sellers = sellers.select(
        "seller_id",
        "seller_city",
        "seller_state",
        "seller_state_name",
    )

    window_spec = Window.orderBy("seller_id")
    dim_sellers = dim_sellers.withColumn("seller_key", row_number().over(window_spec))

    dim_sellers = dim_sellers.select(
        "seller_key", "seller_id", "seller_city", "seller_state", "seller_state_name",
    )

    write_gold_csv(dim_sellers, "dim_sellers", "dim_sellers.csv")
    spark.stop()


if __name__ == "__main__":
    run()