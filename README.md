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

[Bronze]  Raw partitioned tables      → partitioning_strategies.sql
[Silver]  Cleaned + SCD2 dimensions   → scd_type2_bigquery.sql
[Silver]  Incremental fact loads      → incremental_elt_pattern.sql
[Gold]    Aggregated serving tables   → clustering_optimisation.sql
[All]     Quality gates between layers → data_quality_checks.sql

## Why BigQuery-specific patterns matter

BigQuery is serverless — there are no indexes, no vacuuming, no storage tuning knobs.
All performance optimisation happens through:
1. **Partitioning** — limits bytes scanned per query
2. **Clustering** — sorts data within partitions for faster filtering
3. **Incremental processing** — avoids full table scans on every run
4. **MERGE statements** — atomic upserts without delete+insert races

## Author

Shaloo Merin Mathew — Senior Data Engineer  
[LinkedIn](https://www.linkedin.com/in/shaloo-mathew-b50878b8/) · 
[GitHub](https://github.com/shaloommathew-sys)
