import os
import boto3
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession


# AWS Configuration
BUCKET_NAME = "guhan-cloud-batch-pipeline-2026"
AWS_REGION = "eu-north-1"


# Local Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_PATH = PROJECT_ROOT / "Raw_data"


# Load credentials

load_dotenv(PROJECT_ROOT / ".env")

# Spark Session (for silver-layer transforms)
def get_spark_session(app_name="cloud-pipeline"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"]) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"]) \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()


def s3_path(*parts):
    """Build an s3a:// path. Example: s3_path('bronze','customers','file.csv')"""
    return f"s3a://{BUCKET_NAME}/" + "/".join(parts)


def write_layer_csv(df, layer, s3_folder, filename):
    """
    Write a DataFrame as a single, cleanly-named CSV directly to a layer
    (silver/ or gold/) in S3 -- no local disk hop, no Windows path, no
    boto3 file hunting.

    layer:    "silver" or "gold"
    s3_folder: destination folder under that layer, e.g. "products", "dim_date"
    filename:  final file name, e.g. "dim_date.csv"

    Example:
        write_layer_csv(date_df, "gold", "dim_date", "dim_date.csv")
        -> gold/dim_date/dim_date.csv
    """
    temp_prefix = f"_tmp/{layer}_{s3_folder}__{filename}"
    final_key = f"{layer}/{s3_folder}/{filename}"

    df.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv(f"s3a://{BUCKET_NAME}/{temp_prefix}")

    s3 = boto3.client("s3")
    resp = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=temp_prefix)
    objects = resp.get("Contents", [])

    part_key = next((o["Key"] for o in objects if o["Key"].endswith(".csv")), None)
    if part_key is None:
        raise FileNotFoundError(f"No CSV part file found under {temp_prefix}")

    s3.copy_object(
        Bucket=BUCKET_NAME,
        CopySource={"Bucket": BUCKET_NAME, "Key": part_key},
        Key=final_key,
    )

    # clean up the temp Spark output (part file, _SUCCESS, .crc files)
    for obj in objects:
        s3.delete_object(Bucket=BUCKET_NAME, Key=obj["Key"])

    print(f"Uploaded -> {final_key}")


def write_silver_csv(df, s3_folder, filename):
    """Backward-compatible wrapper -- existing silver scripts keep working unchanged."""
    write_layer_csv(df, "silver", s3_folder, filename)


def write_gold_csv(df, s3_folder, filename):
    """Wrapper for gold-layer writes -- dims, facts, and aggregates all use this."""
    write_layer_csv(df, "gold", s3_folder, filename)