from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain

SCRIPTS_BASE = "/opt/airflow/project/scripts"

TRANSFORM_ENTITIES = [
    "category_translation",
    "customers",
    "geolocation",
    "items",
    "orders",
    "payments",
    "products",
    "reviews",
    "sellers",
]

DIM_SCRIPTS = [
    "dim_customers",
    "dim_date",
    "dim_products",
    "dim_sellers",
]

AGGREGATE_SCRIPTS = [
    "agg_category_peformance",
    "agg_seller_performance",
    "monthly_sales_by_state",
]

with DAG(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2026, 7, 30),
    schedule=None,
    catchup=False,
    max_active_tasks=1,   # belt-and-suspenders: nothing runs at the same time, anywhere in the DAG
    tags=["ecommerce", "data-engineering"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    ingest = BashOperator(
        task_id="ingest_to_s3",
        bash_command=f"python {SCRIPTS_BASE}/ingestion/ingest_to_s3.py",
    )

    with TaskGroup("transformations") as transformations:
        transform_tasks = [
            BashOperator(
                task_id=f"transform_{entity}",
                bash_command=f"python {SCRIPTS_BASE}/transformations/{entity}.py",
            )
            for entity in TRANSFORM_ENTITIES
        ]
        chain(*transform_tasks)   # one after another, not parallel

    with TaskGroup("data_modeling") as data_modeling:
        dim_tasks = [
            BashOperator(
                task_id=script,
                bash_command=f"python {SCRIPTS_BASE}/data_modeling/{script}.py",
            )
            for script in DIM_SCRIPTS
        ]

        fact_task = BashOperator(
            task_id="fact_order_items",
            bash_command=f"python {SCRIPTS_BASE}/data_modeling/fact_order_items.py",
        )

        chain(*dim_tasks, fact_task)   # 4 dims one at a time, then fact_order_items

    with TaskGroup("aggregated_tables") as aggregated_tables:
        agg_tasks = [
            BashOperator(
                task_id=script,
                bash_command=f"python {SCRIPTS_BASE}/aggregated_tables/{script}.py",
            )
            for script in AGGREGATE_SCRIPTS
        ]
        chain(*agg_tasks)   # one after another, not parallel

    start >> ingest >> transformations >> data_modeling >> aggregated_tables >> end