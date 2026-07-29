# Naming Conventions

This document outlines the naming conventions used for S3 folders, files, scripts, and columns across the bronze, silver, and gold layers of this pipeline.

## Table of Contents

1. [General Principles](#general-principles)
2. [S3 Layer Naming Conventions](#s3-layer-naming-conventions)
   - [Bronze Rules](#bronze-rules)
   - [Silver Rules](#silver-rules)
   - [Gold Rules](#gold-rules)
3. [Script Naming Conventions](#script-naming-conventions)
4. [Column Naming Conventions](#column-naming-conventions)
   - [Surrogate Keys](#surrogate-keys)
   - [Derived / Flag Columns](#derived--flag-columns)

---

## General Principles

- **Naming style**: `snake_case` — lowercase letters and underscores only, no spaces or camelCase.
- **Language**: English for all folder names, file names, script names, and columns.
- **Consistency over cleverness**: a table's S3 folder name, its file name, and the script that produces it should all share the same root name wherever possible (e.g. `sellers.py` → `silver/sellers/sellers.csv`).

## S3 Layer Naming Conventions

### Bronze Rules

- Path pattern: **`bronze/<entity>/<original_filename>.csv`**
  - `<entity>`: the dataset name (e.g. `customers`, `order_items`, `category_translation`)
  - `<original_filename>`: the exact file name as received from the source, unrenamed
  - Example: `bronze/customers/olist_customers_dataset.csv`
- Bronze is a raw landing zone — file names are never cleaned up or renamed here, so the layer stays a faithful copy of the source.

### Silver Rules

- Path pattern: **`silver/<entity>/<entity>.csv`**
  - `<entity>`: cleaned, business-friendly table name — this is where the original source filename gets dropped in favor of a short, consistent name
  - Example: `silver/customers/customers.csv`, `silver/products/products.csv`
- Exception — order-related tables (`order_items`, `payments`, `reviews`, `orders` itself) are grouped under a shared `silver/orders/` folder, since they're all order-grain data consumed together:
  - `silver/orders/orders.csv`, `silver/orders/order_items.csv`, `silver/orders/payments.csv`, `silver/orders/reviews.csv`

### Gold Rules

- All gold tables use a category prefix that describes the table's role, followed by a business-aligned entity name.
- Path pattern: **`gold/<category>_<entity>/<category>_<entity>.csv`**
  - `<category>`: `dim` (dimension), `fact` (fact table), or `agg` (aggregate table)
  - `<entity>`: descriptive business name (`customers`, `products`, `order_items`, `monthly_sales_by_state`)
  - Examples:
    - `gold/dim_customers/dim_customers.csv` → dimension table for customer data
    - `gold/fact_order_items/fact_order_items.csv` → fact table at order-line-item grain
    - `gold/agg_seller_performance/agg_seller_performance.csv` → pre-aggregated seller metrics

#### Glossary of Category Patterns

| Pattern | Meaning            | Example(s)                                          |
|---------|--------------------|------------------------------------------------------|
| `dim_`  | Dimension table    | `dim_customers`, `dim_products`, `dim_date`          |
| `fact_` | Fact table         | `fact_order_items`                                    |
| `agg_`  | Aggregate table    | `agg_monthly_sales_by_state`, `agg_category_performance` |

## Script Naming Conventions

- All transformation scripts are named after the entity they produce, matching the gold/silver table name without the layer prefix:
  - **`<entity>.py`**
  - Silver scripts live in `Scripts/transformations/`, e.g. `customers.py`, `orders.py`
  - Gold scripts live in `Scripts/transformations_gold/`, e.g. `dim_customers.py`, `fact_order_items.py`, `agg_seller_performance.py`
- Every transformation script exposes a single entry point function:
  - **`run()`** — contains the full read → transform → guardrail → write logic
  - Guarded by `if __name__ == "__main__": run()` so scripts can be imported (e.g. by an Airflow task) without auto-executing
- Ingestion scripts (raw → bronze) live in `Scripts/ingestion/` and follow the same `run()` pattern, e.g. `injest_to_s3.py`

## Column Naming Conventions

### Surrogate Keys

- All primary keys in gold dimension tables use the suffix `_key`.
- Pattern: **`<entity>_key`**
  - Example: `customer_key` → surrogate key in `dim_customers`
  - Example: `date_key` → the one exception to "arbitrary integer" — `dim_date`'s key is a meaningful `yyyyMMdd` integer so it can be filtered directly without a join
- Fact and aggregate tables reference dimensions by their surrogate key only (`customer_key`, `seller_key`, `product_key`), never by the original natural key (`customer_id`, `seller_id`).

### Derived / Flag Columns

- Columns computed by a transformation (not present in the source data) are named descriptively in plain `snake_case`, no special prefix:
  - `data_quality_issue` — flags rows that fail a validation check (used in `orders`, `products`)
  - `is_late_delivery` — boolean flag derived by comparing two dates (used in `fact_order_items`)
  - `delivery_days` — numeric value derived via `datediff()` (used in `fact_order_items`)
- Aggregated measure columns are named `<aggregation>_<measure>` for clarity:
  - `total_revenue`, `avg_review_score`, `avg_delivery_days`, `late_delivery_rate`, `units_sold`
