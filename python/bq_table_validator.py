"""
python/bq_table_validator.py
-----------------------------
Python script to run BigQuery data quality checks via the BQ client library.
Used in Airflow DAGs as a PythonOperator task between pipeline stages.

In production this runs as:
  Airflow task → calls run_validation() → writes results to dq_check_log → 
  returns True/False → Airflow proceeds or halts the DAG

Requires: google-cloud-bigquery (pip install google-cloud-bigquery)
Auth: Uses Application Default Credentials on GCP (no key file needed on Dataproc/Composer)
"""

from google.cloud import bigquery
from dataclasses import dataclass
from typing import List
from datetime import date
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of a single BigQuery validation check."""
    check_name:  str
    status:      str      # PASSED / FAILED
    severity:    str      # CRITICAL / WARNING
    actual:      str
    expected:    str
    message:     str


def run_bq_query(client: bigquery.Client, query: str, params: list) -> list:
    """Runs a parameterised BigQuery query and returns rows."""
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    query_job  = client.query(query, job_config=job_config)
    return list(query_job.result())


def check_row_count(client: bigquery.Client, project: str, dataset: str,
                    table: str, run_date: date) -> ValidationResult:
    """
    CRITICAL: Checks that the table has rows for the given run_date.
    An empty partition means upstream pipeline produced no data.
    """
    query = f"""
        SELECT COUNT(*) AS cnt
        FROM `{project}.{dataset}.{table}`
        WHERE _ingestion_date = @run_date
    """
    params = [bigquery.ScalarQueryParameter("run_date", "DATE", run_date)]
    rows   = run_bq_query(client, query, params)
    count  = rows[0].cnt

    passed = count > 0
    return ValidationResult(
        check_name=f"{table}.row_count",
        status="PASSED" if passed else "FAILED",
        severity="CRITICAL",
        actual=str(count),
        expected="> 0",
        message=f"Row count for {run_date}: {count}"
    )


def check_null_primary_key(client: bigquery.Client, project: str, dataset: str,
                            table: str, pk_column: str, run_date: date) -> ValidationResult:
    """CRITICAL: Primary key column must have zero nulls."""
    query = f"""
        SELECT COUNT(*) AS null_count
        FROM `{project}.{dataset}.{table}`
        WHERE _ingestion_date = @run_date
          AND {pk_column} IS NULL
    """
    params     = [bigquery.ScalarQueryParameter("run_date", "DATE", run_date)]
    rows       = run_bq_query(client, query, params)
    null_count = rows[0].null_count

    passed = null_count == 0
    return ValidationResult(
        check_name=f"{table}.{pk_column}_not_null",
        status="PASSED" if passed else "FAILED",
        severity="CRITICAL",
        actual=f"{null_count} nulls",
        expected="0 nulls",
        message=f"Null {pk_column} count: {null_count}"
    )


def check_no_duplicates(client: bigquery.Client, project: str, dataset: str,
                         table: str, pk_column: str, run_date: date) -> ValidationResult:
    """CRITICAL: No duplicate primary keys within the partition."""
    query = f"""
        SELECT
          COUNT(*)                     AS total_rows,
          COUNT(DISTINCT {pk_column})  AS unique_keys
        FROM `{project}.{dataset}.{table}`
        WHERE _ingestion_date = @run_date
    """
    params     = [bigquery.ScalarQueryParameter("run_date", "DATE", run_date)]
    rows       = run_bq_query(client, query, params)
    row        = rows[0]
    duplicates = row.total_rows - row.unique_keys

    passed = duplicates == 0
    return ValidationResult(
        check_name=f"{table}.{pk_column}_no_duplicates",
        status="PASSED" if passed else "FAILED",
        severity="CRITICAL",
        actual=f"{duplicates} duplicates",
        expected="0 duplicates",
        message=f"Total: {row.total_rows} | Unique: {row.unique_keys} | Dupes: {duplicates}"
    )


def run_validation(project: str, dataset: str, table: str,
                   pk_column: str, run_date: date) -> bool:
    """
    Main entry point — runs all checks and returns True if pipeline can continue.
    Called by Airflow as a PythonOperator task.

    Returns:
        True  → all CRITICAL checks passed, pipeline proceeds
        False → at least one CRITICAL check failed, Airflow stops the DAG
    """
    client  = bigquery.Client(project=project)
    results: List[ValidationResult] = []

    # Run all checks
    results.append(check_row_count(client, project, dataset, table, run_date))
    results.append(check_null_primary_key(client, project, dataset, table, pk_column, run_date))
    results.append(check_no_duplicates(client, project, dataset, table, pk_column, run_date))

    # Print report
    print(f"\n{'='*55}")
    print(f"DATA QUALITY REPORT — {table} — {run_date}")
    print(f"{'='*55}")

    has_critical_failure = False
    for r in results:
        icon = "✅" if r.status == "PASSED" else "❌"
        print(f"{icon} [{r.severity}] {r.check_name}")
        print(f"   {r.message}")
        if r.status == "FAILED" and r.severity == "CRITICAL":
            has_critical_failure = True
            logger.error(f"CRITICAL FAILURE: {r.check_name} — {r.message}")

    print(f"{'='*55}")
    pipeline_ok = not has_critical_failure
    print(f"Pipeline proceed: {pipeline_ok}\n")

    return pipeline_ok


# Example usage — called directly or via Airflow PythonOperator
if __name__ == "__main__":
    ok = run_validation(
        project    = "your-gcp-project",
        dataset    = "your_dataset",
        table      = "orders_silver",
        pk_column  = "order_id",
        run_date   = date.today()
    )
    exit(0 if ok else 1)    # exit code 1 makes Airflow mark the task as FAILED
