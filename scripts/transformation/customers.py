import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import get_spark_session, s3_path, write_silver_csv
from pyspark.sql.functions import col, trim, lower, create_map, lit
from itertools import chain

BRAZIL_STATES = {
    "SC": "Santa Catarina", "RO": "Rondônia", "PI": "Piauí", "AM": "Amazonas",
    "RR": "Roraima", "GO": "Goiás", "TO": "Tocantins", "MT": "Mato Grosso",
    "SP": "São Paulo", "ES": "Espírito Santo", "PB": "Paraíba",
    "RS": "Rio Grande do Sul", "MS": "Mato Grosso do Sul", "AL": "Alagoas",
    "MG": "Minas Gerais", "PA": "Pará", "BA": "Bahia", "SE": "Sergipe",
    "PE": "Pernambuco", "CE": "Ceará", "RN": "Rio Grande do Norte",
    "RJ": "Rio de Janeiro", "MA": "Maranhão", "AC": "Acre",
    "DF": "Distrito Federal", "PR": "Paraná", "AP": "Amapá",
}


def run():
    spark = get_spark_session("bronze-to-silver-customers")

    customers_df = spark.read.csv(
        s3_path("bronze", "customers", "olist_customers_dataset.csv"),
        header=True, inferSchema=True,
    )

    customers_df = (
        customers_df
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("customer_unique_id", trim(col("customer_unique_id")))
        .withColumn("customer_city", lower(trim(col("customer_city"))))
        .withColumn("customer_state", trim(col("customer_state")))
    )

    mapping_expr = create_map([lit(x) for x in chain(*BRAZIL_STATES.items())])
    customers_df = customers_df.withColumn(
        "customer_state", mapping_expr[col("customer_state")]
    )

    write_silver_csv(customers_df, "customers", "customers.csv")
    spark.stop()


if __name__ == "__main__":
    run()
