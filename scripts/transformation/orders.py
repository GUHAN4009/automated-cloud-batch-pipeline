import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import get_spark_session, s3_path, write_silver_csv
from pyspark.sql.functions import col, trim, when


def run():
    spark = get_spark_session("bronze-to-silver-orders")

    orders_df = spark.read.csv(
        s3_path("bronze", "orders", "olist_orders_dataset.csv"),
        header=True, inferSchema=True,
    )

    silver_orders = (
        orders_df
        .withColumn("order_id", trim(col("order_id")))
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("order_status", trim(col("order_status")))
    )

    silver_orders = silver_orders.withColumn(
        "data_quality_issue",
        when(
            (col("order_status") == "delivered") &
            (
                col("order_approved_at").isNull() |
                col("order_delivered_carrier_date").isNull() |
                col("order_delivered_customer_date").isNull()
            ),
            "MISSING_DELIVERY_INFORMATION",
        ).otherwise("OK"),
    )

    write_silver_csv(silver_orders, "orders", "orders.csv")
    spark.stop()


if __name__ == "__main__":
    run()
