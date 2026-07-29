import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import get_spark_session, s3_path, write_silver_csv
from pyspark.sql.functions import col, trim


def run():
    spark = get_spark_session("bronze-to-silver-reviews")

    reviews = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .csv(s3_path("bronze", "order_reviews", "olist_order_reviews_dataset.csv"))
    )

    out_of_range = reviews.filter((col("review_score") < 1) | (col("review_score") > 5)).count()
    if out_of_range:
        raise ValueError(f"Data quality failure: {out_of_range} review_score values outside 1-5")

    reviews = (
        reviews
        .withColumn("review_id", trim(col("review_id")))
        .withColumn("order_id", trim(col("order_id")))
        .withColumn("review_comment_title", trim(col("review_comment_title")))
        .withColumn("review_comment_message", trim(col("review_comment_message")))
    )

    write_silver_csv(reviews, "orders", "reviews.csv")
    spark.stop()


if __name__ == "__main__":
    run()
