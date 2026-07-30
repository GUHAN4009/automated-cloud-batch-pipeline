# Airflow Setup Guide — Ecommerce Cloud Batch Pipeline

This guide walks through setting up and running the Airflow orchestration layer for this project, using the exact folder structure the `docker-compose.yml` file expects.

---

## 1. Required Folder Layout

The compose file mounts folders **relative to itself**, so where you place `docker-compose.yml` matters. Your project must look like this:

```
project-root/
├── Raw_data/                     ← sibling of docker/  (../Raw_data mount)
│   ├── olist_customers_dataset.csv
│   ├── olist_geolocation_dataset.csv
│   ├── olist_order_items_dataset.csv
│   ├── olist_order_payments_dataset.csv
│   ├── olist_order_reviews_dataset.csv
│   ├── olist_orders_dataset.csv
│   ├── olist_products_dataset.csv
│   ├── olist_sellers_dataset.csv
│   └── product_category_name_translation.csv
│
├── scripts/                       ← sibling of docker/  (../scripts mount)
│   ├── config.py
│   ├── ingestion/
│   │   └── ingest_to_s3.py
│   ├── transformations/
│   │   ├── category_translation.py
│   │   ├── customers.py
│   │   ├── geolocation.py
│   │   ├── items.py
│   │   ├── orders.py
│   │   ├── payments.py
│   │   ├── products.py
│   │   ├── reviews.py
│   │   └── sellers.py
│   ├── data_modeling/
│   │   ├── dim_customers.py
│   │   ├── dim_date.py
│   │   ├── dim_products.py
│   │   ├── dim_sellers.py
│   │   └── fact_order_items.py
│   └── aggregated_tables/
│       ├── agg_category_peformance.py
│       ├── agg_seller_performance.py
│       └── monthly_sales_by_state.py
│
└── docker/                        ← docker-compose.yml lives here
    ├── Dockerfile
    ├── docker-compose.yml
    ├── .env                       ← you create this (Step 2)
    ├── dags/
    │   └── ecommerce_pipeline.py
    ├── logs/                      ← empty folder, Airflow writes here
    ├── plugins/                   ← empty folder, optional custom plugins
    └── config/                    ← empty folder, optional airflow.cfg overrides
```

**Why this matters:** in `docker-compose.yml`, the `x-airflow-common` volumes block mounts `dags`, `logs`, `plugins`, and `config` from the **same directory** as the compose file, but reaches **one level up** for `../scripts` and `../Raw_data`. If `scripts/` and `Raw_data/` aren't siblings of the `docker/` folder, the containers won't find them and every `BashOperator` task will fail with a "file not found" error.

If you'd rather not create `logs/`, `plugins/`, and `config/` by hand, Docker Compose will auto-create them as empty directories on first run — but on Linux they'll be owned by `root` unless `AIRFLOW_UID` is set correctly (Step 2).

---

## 2. Create the `.env` File

Inside the **`docker/`** folder (next to `docker-compose.yml`), create a file named `.env`:

```env
# Maps your host user into the containers so mounted files aren't owned by root
AIRFLOW_UID=50000

# AWS credentials -- used by both boto3 (ingestion) and Spark's s3a connector
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_DEFAULT_REGION=eu-north-1

# Optional -- override the default Airflow admin login
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
```

On Linux, find your real UID with:

```bash
id -u
```

and use that value instead of `50000` if it differs, so file permissions on `logs/` etc. line up correctly with your host user.

> ⚠️ Never commit this `.env` file — add `docker/.env` to `.gitignore`.

---

## 3. Point `config.py` at Your Bucket

Before starting the cluster, open `scripts/config.py` and confirm these match your own AWS setup:

```python
BUCKET_NAME = "guhan-cloud-batch-pipeline-2026"   # ← change to your bucket name
AWS_REGION = "eu-north-1"                          # ← change to your bucket's region
```

The bucket must already exist in S3 — the pipeline writes to `bronze/`, `silver/`, and `gold/` prefixes inside it but does not create the bucket itself.

---

## 4. First-Time Initialization

From inside the `docker/` folder:

```bash
cd docker

# Builds the custom image (Airflow + Java 17 + PySpark + boto3)
docker compose build

# Runs DB migrations and creates the admin user (one-time step)
docker compose up airflow-init
```

Watch the output for the resource warnings (memory/CPU/disk) — if you see them, Docker Desktop likely needs more resources allocated (Airflow's reference setup recommends ≥4GB RAM, ≥2 CPUs, ≥10GB free disk).

You should see the `airflow-init` container exit with code `0` when it's done successfully.

---

## 5. Start the Cluster

```bash
docker compose up -d
```

This brings up, in the background:

| Service | Purpose |
|---|---|
| `postgres` | Airflow metadata DB |
| `redis` | Celery broker |
| `airflow-webserver` | UI at `localhost:8080` |
| `airflow-scheduler` | Parses the DAG, schedules tasks |
| `airflow-worker` | Executes each `BashOperator` task |
| `airflow-triggerer` | Handles deferrable/async operators |

Check everyone is healthy:

```bash
docker compose ps
```

All services should show `healthy` (this can take 30–60 seconds on first boot).

---

## 6. Access the Airflow UI

Open **http://localhost:8080** and log in with the username/password from your `.env` (default `airflow` / `airflow`).

You should see a DAG named **`ecommerce_pipeline`** in the list, tagged `ecommerce`, `data-engineering`. It loads from `docker/dags/ecommerce_pipeline.py`, which is mounted straight into the scheduler and webserver containers — any edits you make to that file locally will show up in the UI within a few seconds (no rebuild needed).

The DAG is set to `schedule=None`, so it will **not** run automatically — it's meant to be triggered manually.

---

## 7. Trigger a Run

1. Click into the `ecommerce_pipeline` DAG.
2. Un-pause it (toggle switch, top-left) if it's paused — new DAGs start paused by default.
3. Click the **▶ Trigger DAG** button.
4. Switch to the **Graph** view to watch tasks execute in order:

```
start → ingest_to_s3 → transformations (9 tasks) → data_modeling (4 dims + fact) → aggregated_tables (3 tasks) → end
```

Because `max_active_tasks=1` is set on the DAG, everything runs strictly one task at a time — you'll see one node turn green before the next one starts. This is intentional and makes it easy to tell exactly which script failed if something goes wrong.

---

## 8. Reading Logs / Debugging a Failed Task

If a task turns red:

1. Click the task's box in the Graph view.
2. Click **Logs**.
3. Since every task is just `python {SCRIPTS_BASE}/<subfolder>/<script>.py` running inside the worker container, the log will show the actual Python/Spark stack trace (missing CSV, S3 auth error, data-quality `ValueError`, etc.) exactly as it would locally.

Common first-run issues and where to look:

| Symptom | Likely cause |
|---|---|
| `ingest_to_s3` fails, "File not found" | `Raw_data/` isn't a sibling of `docker/`, or a CSV is missing/misnamed |
| Any Spark task fails with an S3 auth/403 error | `.env` AWS credentials wrong, or `config.py`'s `BUCKET_NAME`/`AWS_REGION` don't match your bucket |
| `fact_order_items` fails, missing dimension columns | One of the 4 dimension tasks failed earlier in the same run — check `data_modeling` group first |
| Task fails instantly with `ModuleNotFoundError: pyspark` | Image wasn't rebuilt after a Dockerfile change — run `docker compose build` again |
| `.py` file edits not showing up in the DAG | You edited a file outside `docker/dags/`, `../scripts/`, etc. — confirm the file is inside a mounted path |

---

## 9. Stopping / Resetting

```bash
# Stop the cluster, keep data (Postgres volume, logs)
docker compose down

# Stop and wipe everything, including the Postgres metadata volume
docker compose down --volumes
```

Use the second form if you want a completely clean slate (e.g. DAG history, connections, and users get wiped along with it).

---

## 10. Optional: Celery Monitoring (Flower)

```bash
docker compose --profile flower up -d
```

Then visit **http://localhost:5555** to see live worker/task status through Celery's Flower dashboard.

---

## Quick Reference

```bash
cd docker
docker compose build                 # build the custom image
docker compose up airflow-init       # one-time DB + user setup
docker compose up -d                 # start the cluster
docker compose ps                    # check service health
docker compose logs -f airflow-worker  # tail worker logs live
docker compose down                  # stop everything
```
