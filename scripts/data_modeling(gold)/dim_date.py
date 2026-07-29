import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import get_spark_session, write_gold_csv
from pyspark.sql.functions import year, month, quarter, dayofweek, when, date_format


def run():
    spark = get_spark_session("silver-to-gold-dim-date")

    date_df = spark.sql("""
        SELECT explode(sequence(
            to_date('2016-09-01'),
            to_date('2018-12-31'),
            interval 1 day
        )) AS full_date
    """)

    date_df = (
        date_df
        .withColumn("year", year("full_date"))
        .withColumn("month", month("full_date"))
        .withColumn("quarter", quarter("full_date"))
        .withColumn("day_of_week", dayofweek("full_date"))
        .withColumn("is_weekend", when(dayofweek("full_date").isin(1, 7), True).otherwise(False))
        .withColumn("date_key", date_format("full_date", "yyyyMMdd").cast("int"))
    )

    write_gold_csv(date_df, "dim_date", "dim_date.csv")
    spark.stop()


if __name__ == "__main__":
    run()