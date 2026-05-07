# BigQuery Pipeline Patterns

Production-ready BigQuery SQL patterns used in real-world GCP data engineering projects.
Covers partitioning strategies, clustering, incremental ELT, SCD Type 2, data quality
checks, and query optimisation — all with detailed explanations.

## What's covered

| File | Pattern |
|---|---|
| `sql/partitioning_strategies.sql` | Date, range, and ingestion-time partitioning |
| `sql/clustering_optimisation.sql` | Clustering design + cost reduction techniques |
| `sql/incremental_elt_pattern.sql` | Watermark-based incremental loads (idempotent) |
| `sql/scd_type2_bigquery.sql` | Slowly Changing Dimension Type 2 using MERGE |
| `sql/data_quality_checks.sql` | Row-level validation, null checks, anomaly detection |
| `sql/window_functions.sql` | Ranking, running totals, LAG/LEAD patterns |
| `python/bq_table_validator.py` | Python script to run quality checks via BigQuery client |

## Architecture context

These patterns map directly to a Medallion Lakehouse on GCP:

```
