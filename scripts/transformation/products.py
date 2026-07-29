import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import get_spark_session, s3_path, write_silver_csv
from pyspark.sql.functions import col, trim, lower, when


def run():
    spark = get_spark_session("bronze-to-silver-products")

    products_df = spark.read.csv(
        s3_path("bronze", "products", "olist_products_dataset.csv"),
        header=True, inferSchema=True,
    )

    products_df = (
        products_df
        .withColumnRenamed("product_name_lenght", "product_name_length")
        .withColumnRenamed("product_description_lenght", "product_description_length")
        .withColumn("product_id", trim(col("product_id")))
        .withColumn("product_category_name", lower(trim(col("product_category_name"))))
    )

    products_df = products_df.withColumn(
        "product_category_name",
        when(col("product_category_name").isNull(), "Unknown")
        .otherwise(col("product_category_name")),
    )

    # previously identified but unhandled: null product_weight_g -- flag rather
    # than silently ship rows with missing weight (matters for shipping/freight calcs)
    products_df = products_df.withColumn(
        "data_quality_issue",
        when(col("product_weight_g").isNull(), "MISSING_WEIGHT").otherwise("OK"),
    )

    write_silver_csv(products_df, "products", "products.csv")
    spark.stop()


if __name__ == "__main__":
    run()
