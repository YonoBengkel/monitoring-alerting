import csv
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from notification_utils import (
    task_failure_callback,
    task_retry_callback,
    dag_failure_callback,
    record_run_history,
)

DATA_DIR = Path("/opt/airflow/data")
INPUT_FILE = DATA_DIR / "input" / "transactions.csv"
OUTPUT_DIR = DATA_DIR / "output"
METRICS_DIR = DATA_DIR / "metrics"
OUTPUT_FILE = OUTPUT_DIR / "transactions_clean.csv"
SUMMARY_FILE = OUTPUT_DIR / "sales_summary.json"
METRICS_FILE = METRICS_DIR / "latest_metrics.json"

logger = logging.getLogger(__name__)

def extract_data(**context):
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    METRICS_DIR.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now(timezone.utc).isoformat()
    context["ti"].xcom_push(key="pipeline_started_at", value=started_at)

    with INPUT_FILE.open(newline="", encoding="utf-8") as f:
        records = list(csv.DictReader(f))
    if not records:
        raise ValueError("Input CSV has no transaction records")
    logger.info("Extracted %s transaction rows", len(records))
    return {"row_count": len(records), "source_file": str(INPUT_FILE)}

def transform_data(**context):
    extract_info = context["ti"].xcom_pull(task_ids="extract_data")
    if not extract_info:
        raise RuntimeError("Extract task metadata not found in XCom")

    # Toggle from Airflow UI: Admin > Variables > simulate_failure = true
    simulate_failure = Variable.get("simulate_failure", default_var="false").lower() == "true"
    if simulate_failure:
        raise ValueError("SIMULATED FAILURE: transform task deliberately failed")

    with INPUT_FILE.open(newline="", encoding="utf-8") as f:
        source_rows = list(csv.DictReader(f))

    clean_rows = []
    for row in source_rows:
        quantity = int(row["quantity"])
        unit_price = float(row["unit_price"])
        if quantity <= 0 or unit_price < 0:
            raise ValueError(f"Invalid quantity or unit price in {row['transaction_id']}")
        row["revenue"] = f"{quantity * unit_price:.2f}"
        clean_rows.append(row)

    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as f:
        fields = list(clean_rows[0].keys())
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(clean_rows)

    logger.info("Transformed %s rows; output=%s", len(clean_rows), OUTPUT_FILE)
    return {"row_count": len(clean_rows), "output_file": str(OUTPUT_FILE)}

def load_data(**context):
    # Artificial delay can be set from Airflow Variables for duration testing.
    slow_seconds = int(Variable.get("simulate_slow_seconds", default_var="0"))
    if slow_seconds > 0:
        logger.info("Simulating slow pipeline: sleeping %s seconds", slow_seconds)
        time.sleep(min(slow_seconds, 120))

    with OUTPUT_FILE.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    total_revenue = sum(float(row["revenue"]) for row in rows)
    summary = {
        "loaded_at_utc": datetime.now(timezone.utc).isoformat(),
        "record_count": len(rows),
        "total_revenue": round(total_revenue, 2),
        "currency": "IDR",
    }
    SUMMARY_FILE.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    logger.info("Loaded %s records; total revenue=%s IDR", len(rows), total_revenue)
    return summary

def evaluate_metrics(**context):
    ti = context["ti"]
    started_at = ti.xcom_pull(task_ids="extract_data", key="pipeline_started_at")
    if not started_at:
        raise RuntimeError("Pipeline start timestamp not found")
    start = datetime.fromisoformat(started_at)
    end = datetime.now(timezone.utc)
    duration_seconds = (end - start).total_seconds()

    with INPUT_FILE.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    latencies = []
    for row in rows:
        expected = datetime.fromisoformat(row["expected_available_at"]).replace(tzinfo=timezone.utc)
        actual = datetime.fromisoformat(row["actual_available_at"]).replace(tzinfo=timezone.utc)
        latencies.append((actual - expected).total_seconds())

    average_latency = sum(latencies) / len(latencies) if latencies else 0
    max_latency = max(latencies) if latencies else 0
    duration_threshold = 15 * 60
    latency_threshold = 10 * 60

    metrics = {
        "dag_id": context["dag"].dag_id,
        "run_id": ti.run_id,
        "measured_at_utc": end.isoformat(),
        "execution_duration_seconds": round(duration_seconds, 2),
        "duration_threshold_seconds": duration_threshold,
        "average_data_latency_seconds": round(average_latency, 2),
        "max_data_latency_seconds": round(max_latency, 2),
        "latency_threshold_seconds": latency_threshold,
        "records_processed": len(rows),
        "duration_alert": duration_seconds > duration_threshold,
        "latency_alert": max_latency > latency_threshold,
        "note": "Latency uses sample expected_available_at and actual_available_at fields.",
    }
    METRICS_DIR.mkdir(parents=True, exist_ok=True)
    METRICS_FILE.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    logger.info("PIPELINE METRICS: %s", json.dumps(metrics))

    if metrics["duration_alert"] or metrics["latency_alert"]:
        from notification_utils import send_alert
        reasons = []
        if metrics["duration_alert"]:
            reasons.append(f"Execution duration {duration_seconds:.0f}s exceeds {duration_threshold}s")
        if metrics["latency_alert"]:
            reasons.append(f"Maximum data latency {max_latency:.0f}s exceeds {latency_threshold}s")
        send_alert(
            f"DAG: `{context['dag'].dag_id}`\n"
            f"Run: `{ti.run_id}`\n"
            + "\n".join(reasons),
            "WARNING",
        )
    return metrics

with DAG(
    dag_id="etl_monitoring_demo",
    description="Sales ETL with task callbacks, metrics and webhook alerts",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    on_failure_callback=dag_failure_callback,
    on_success_callback=record_run_history,
    default_args={
        "owner": "data-engineering-training",
        "retries": 1,
        "retry_delay": __import__("datetime").timedelta(seconds=30),
        "on_failure_callback": task_failure_callback,
        "on_retry_callback": task_retry_callback,
    },
    tags=["training", "monitoring", "alerting"],
) as dag:
    extract = PythonOperator(task_id="extract_data", python_callable=extract_data)
    transform = PythonOperator(task_id="transform_data", python_callable=transform_data)
    load = PythonOperator(task_id="load_data", python_callable=load_data)
    metrics = PythonOperator(task_id="evaluate_metrics", python_callable=evaluate_metrics)

    extract >> transform >> load >> metrics
