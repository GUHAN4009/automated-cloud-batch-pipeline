import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import get_spark_session, s3_path, write_gold_csv
from pyspark.sql.functions import col, sum as spark_sum, datediff, date_format, when, first


def run():
    spark = get_spark_session("silver-to-gold-fact-order-items")

    order_items = spark.read.csv(s3_path("silver", "orders", "order_items.csv"), header=True, inferSchema=True)
    orders = spark.read.csv(s3_path("silver", "orders", "orders.csv"), header=True, inferSchema=True)
    payments = spark.read.csv(s3_path("silver", "orders", "payments.csv"), header=True, inferSchema=True)
    reviews = spark.read.csv(s3_path("silver", "orders", "reviews.csv"), header=True, inferSchema=True)

    dim_customers = spark.read.csv(s3_path("gold", "dim_customers", "dim_customers.csv"), header=True, inferSchema=True)
    dim_sellers = spark.read.csv(s3_path("gold", "dim_sellers", "dim_sellers.csv"), header=True, inferSchema=True)
    dim_products = spark.read.csv(s3_path("gold", "dim_products", "dim_products.csv"), header=True, inferSchema=True)

    # --- collapse payments to one row per order BEFORE joining (avoids fan-out) ---
    payments_per_order = payments.groupBy("order_id").agg(
        spark_sum("payment_value").alias("payment_value")
    )

    # --- collapse reviews to one row per order (an order can have >1 review) ---
    reviews_per_order = reviews.groupBy("order_id").agg(
        first("review_score").alias("review_score")
    )

    # --- start from order_items (the grain) and bring in order-level info ---
    fact = order_items.join(orders, on="order_id", how="left")
    fact = fact.join(payments_per_order, on="order_id", how="left")
    fact = fact.join(reviews_per_order, on="order_id", how="left")

    # --- bring in surrogate keys from each dimension ---
    fact = fact.join(dim_customers.select("customer_id", "customer_key"), on="customer_id", how="left")
    fact = fact.join(dim_sellers.select("seller_id", "seller_key"), on="seller_id", how="left")
    fact = fact.join(dim_products.select("product_id", "product_key"), on="product_id", how="left")

    # --- derive delivery metrics ---
    fact = fact.withColumn(
        "delivery_days",
        datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp")),
    )
    fact = fact.withColumn(
        "is_late_delivery",
        when(
            col("order_delivered_customer_date") > col("order_estimated_delivery_date"),
            True,
        ).otherwise(False),
    )

    # --- date keys, to join against dim_date later ---
    fact = fact.withColumn(
        "order_date_key",
        date_format(col("order_purchase_timestamp"), "yyyyMMdd").cast("int"),
    )

    fact = fact.select(
        "order_id", "order_item_id",
        "customer_key", "seller_key", "product_key",
        "order_date_key",
        "price", "freight_value", "payment_value",
        "delivery_days", "is_late_delivery",
        "review_score", "order_status", "data_quality_issue",
    )

    write_gold_csv(fact, "fact_order_items", "fact_order_items.csv")
    spark.stop()


if __name__ == "__main__":
    run()