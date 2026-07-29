import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import get_spark_session, s3_path, write_silver_csv
from pyspark.sql.functions import col, trim, when


def run():
    spark = get_spark_session("bronze-to-silver-payments")

    payments = spark.read.csv(
        s3_path("bronze", "order_payments", "olist_order_payments_dataset.csv"),
        header=True, inferSchema=True,
    )

    negative_value = payments.filter(col("payment_value") < 0).count()
    if negative_value:
        raise ValueError(f"Data quality failure: {negative_value} negative payment values")

    payments = payments.withColumn(
        "payment_installments",
        when(
            (col("payment_type") == "credit_card") & (col("payment_installments") == 0),
            1,
        ).otherwise(col("payment_installments")),
    )

    payments = (
        payments
        .withColumn("order_id", trim(col("order_id")))
        .withColumn("payment_type", trim(col("payment_type")))
    )

    write_silver_csv(payments, "orders", "payments.csv")
    spark.stop()


if __name__ == "__main__":
    run()
