#Purpose of this script:
#===============================================================================
#This script loads the raw data to the cloud
#================================================================================

import boto3
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))  # points to Scripts/

from config import BUCKET_NAME, RAW_DATA_PATH  # fixed: was "from Scripts.config import ..."

s3 = boto3.client("s3")

FILE_MAPPING = {
    "olist_customers_dataset.csv": "bronze/customers/olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv": "bronze/geolocation/olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv": "bronze/order_items/olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv": "bronze/order_payments/olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv": "bronze/order_reviews/olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv": "bronze/orders/olist_orders_dataset.csv",
    "olist_products_dataset.csv": "bronze/products/olist_products_dataset.csv",
    "olist_sellers_dataset.csv": "bronze/sellers/olist_sellers_dataset.csv",
    "product_category_name_translation.csv": "bronze/category_translation/product_category_name_translation.csv",
}


def run():
    for filename, s3_key in FILE_MAPPING.items():
        local_file = RAW_DATA_PATH / filename

        if not local_file.exists():
            print(f"File not found: {filename}")
            continue

        try:
            print(f"Uploading {filename}...")
            s3.upload_file(Filename=str(local_file), Bucket=BUCKET_NAME, Key=s3_key)
            print(f"Uploaded -> {s3_key}")
        except Exception as e:
            print(f"Failed to upload {filename}")
            print(e)


if __name__ == "__main__":
    run()
