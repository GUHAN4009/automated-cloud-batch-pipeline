#purpose of this script:
#===========================================================================
#read items data from AWS(s3), and tranform it respected to bussines requirements and decisions
#============================================================================

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import get_spark_session, s3_path, write_silver_csv
from pyspark.sql.functions import col, trim


def run():
    spark = get_spark_session("bronze-to-silver-items")

    items = spark.read.csv(
        s3_path("bronze", "order_items", "olist_order_items_dataset.csv"),
        header=True, inferSchema=True,
    )

    # enforce, don't just eyeball: fail the run if bad data shows up
    negative_price = items.filter(col("price") < 0).count()
    negative_freight = items.filter(col("freight_value") < 0).count()
    if negative_price or negative_freight:
        raise ValueError(
            f"Data quality failure: {negative_price} negative prices, "
            f"{negative_freight} negative freight values"
        )

    items = (
        items
        .withColumn("order_id", trim(col("order_id")))
        .withColumn("product_id", trim(col("product_id")))
        .withColumn("seller_id", trim(col("seller_id")))
    )

    # note: this lands in silver/orders/ (not silver/order_items/) by design --
    # order_items, payments, and reviews all group under the orders prefix.
    write_silver_csv(items, "orders", "order_items.csv")
    spark.stop()


if __name__ == "__main__":
    run()
