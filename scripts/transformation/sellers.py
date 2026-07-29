import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import get_spark_session, s3_path, write_silver_csv
from pyspark.sql.functions import col, trim, when, create_map, lit
from itertools import chain

STATE_MAPPING = {
    "AC": "Acre", "AL": "Alagoas", "AP": "Amapá", "AM": "Amazonas",
    "BA": "Bahia", "CE": "Ceará", "DF": "Distrito Federal", "ES": "Espírito Santo",
    "GO": "Goiás", "MA": "Maranhão", "MT": "Mato Grosso", "MS": "Mato Grosso do Sul",
    "MG": "Minas Gerais", "PA": "Pará", "PB": "Paraíba", "PR": "Paraná",
    "PE": "Pernambuco", "PI": "Piauí", "RJ": "Rio de Janeiro", "RN": "Rio Grande do Norte",
    "RS": "Rio Grande do Sul", "RO": "Rondônia", "RR": "Roraima", "SC": "Santa Catarina",
    "SP": "São Paulo", "SE": "Sergipe", "TO": "Tocantins",
}


def run():
    spark = get_spark_session("bronze-to-silver-sellers")

    sellers_df = spark.read.csv(
        s3_path("bronze", "sellers", "olist_sellers_dataset.csv"),
        header=True, inferSchema=True,
    )

    # known bad sentinel value found during profiling
    sellers_df = sellers_df.withColumn(
        "seller_city",
        when(col("seller_city") == "04482255", "Unknown").otherwise(col("seller_city")),
    )

    mapping_expr = create_map([lit(x) for x in chain(*STATE_MAPPING.items())])
    sellers_df = sellers_df.withColumn("seller_state_name", mapping_expr[col("seller_state")])

    sellers_df = (
        sellers_df
        .withColumn("seller_id", trim(col("seller_id")))
        .withColumn("seller_city", trim(col("seller_city")))
        .withColumn("seller_state", trim(col("seller_state")))
    )

    write_silver_csv(sellers_df, "sellers", "sellers.csv")
    spark.stop()


if __name__ == "__main__":
    run()
