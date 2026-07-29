#purpose of this script:
#===========================================================================
#read category_translation.csv data from AWS(s3), and tranform it respected to bussines requirements and decisions
#============================================================================
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import get_spark_session, s3_path, write_silver_csv
from pyspark.sql.functions import col, trim


def run():
    spark = get_spark_session("bronze-to-silver-category-translation")

    product_cat = spark.read.csv(
        s3_path("bronze", "category_translation", "product_category_name_translation.csv"),
        header=True, inferSchema=True,
    )

    product_cat = (
        product_cat
        .withColumn("product_category_name", trim(col("product_category_name")))
        .withColumn("product_category_name_english", trim(col("product_category_name_english")))
    )

    write_silver_csv(product_cat, "products_cat", "product_cat.csv")
    spark.stop()


if __name__ == "__main__":
    run()
