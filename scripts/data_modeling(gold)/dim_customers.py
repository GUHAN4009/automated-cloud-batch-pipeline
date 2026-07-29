import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import get_spark_session, s3_path, write_gold_csv
from pyspark.sql import Window
from pyspark.sql.functions import row_number


def run():
    spark = get_spark_session("silver-to-gold-dim-customers")

    customers = spark.read.csv(
        s3_path("silver", "customers", "customers.csv"),
        header=True, inferSchema=True,
    )

    # keep only what a dimension needs: identifiers + descriptive context.
    # no price/count/measure columns belong here -- those live in the fact table.
    dim_customers = customers.select(
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state",
    )

    # surrogate key: sort by the natural key, then number the rows 1, 2, 3...
    window_spec = Window.orderBy("customer_id")
    dim_customers = dim_customers.withColumn("customer_key", row_number().over(window_spec))

    # putting the surrogate key first -- convention, makes the table easy to scan
    dim_customers = dim_customers.select(
        "customer_key", "customer_id", "customer_unique_id",
        "customer_city", "customer_state",
    )

    write_gold_csv(dim_customers, "dim_customers", "dim_customers.csv")
    spark.stop()


if __name__ == "__main__":
    run()