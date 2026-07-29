import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import get_spark_session, s3_path, write_gold_csv
from pyspark.sql import Window
from pyspark.sql.functions import row_number


def run():
    spark = get_spark_session("silver-to-gold-dim-products")

    products = spark.read.csv(
        s3_path("silver", "products", "products.csv"),
        header=True, inferSchema=True,
    )
    category_translation = spark.read.csv(
        s3_path("silver", "products_cat", "product_cat.csv"),
        header=True, inferSchema=True,
    )

    dim_products = products.join(
        category_translation,
        on="product_category_name",   # the shared column both tables have
        how="left",                    # keep every product, even if no translation matches
    )

    dim_products = dim_products.select(
        "product_id",
        "product_category_name",
        "product_category_name_english",
        "product_weight_g",
    )

    window_spec = Window.orderBy("product_id")
    dim_products = dim_products.withColumn("product_key", row_number().over(window_spec))

    dim_products = dim_products.select(
        "product_key", "product_id", "product_category_name",
        "product_category_name_english", "product_weight_g",
    )

    write_gold_csv(dim_products, "dim_products", "dim_products.csv")
    spark.stop()


if __name__ == "__main__":
    run()