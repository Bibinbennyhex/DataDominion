# Summary Pipeline v9 - Local Testing Guide

## Overview

This setup uses the `tabulario/spark-iceberg` image with MinIO for S3-compatible storage and Iceberg REST catalog.

## Services

| Service | Port | Description |
|---------|------|-------------|
| spark-iceberg | 8888 | Jupyter Notebooks |
| spark-iceberg | 8080 | Spark Master UI |
| spark-iceberg | 4040-4042 | Spark Application UI |
| iceberg-rest | 8181 | Iceberg REST Catalog |
| minio | 9000 | S3-compatible storage |
| minio | 9001 | MinIO Console |

## Quick Start

### 1. Start Services
```bash
cd updates/summary_v9/local/base
docker-compose up -d
```

### 2. Wait for Services
```bash
# Check all services are running
docker-compose ps

# View logs
docker-compose logs -f spark-iceberg
```

### 3. Access Jupyter Notebook
Open http://localhost:8888 in your browser.

### 4. Run Pipeline in Notebook

```python
# Import pipeline
import sys
sys.path.append('/home/iceberg/pipeline')
from summary_pipeline import run_pipeline, PipelineConfig

# Create SparkSession (pre-configured in notebook)
# spark is already available

# Configure for local testing
config = PipelineConfig()
config.accounts_table = "demo.accounts"
config.summary_table = "demo.summary"
config.latest_summary_table = "demo.latest_summary"

# Run pipeline
stats = run_pipeline(spark, config)
print(stats)
```

### 5. Run Tests

```bash
# Enter container
docker-compose exec spark-iceberg bash

# Navigate to test directory
cd /home/iceberg/test

# Run test scenarios
python test_scenarios.py
```

### 6. Stop Services
```bash
docker-compose down

# Remove volumes too
docker-compose down -v
```

---

## Directory Mounts

| Host Path | Container Path | Purpose |
|-----------|---------------|---------|
| `../../base` | `/home/iceberg/pipeline` | Pipeline code |
| `../../test` | `/home/iceberg/test` | Test scenarios |
| `./warehouse` | `/home/iceberg/warehouse` | Iceberg warehouse |
| `./notebooks` | `/home/iceberg/notebooks/notebooks` | Jupyter notebooks |
| `./data` | `/home/iceberg/data` | Test data |

---

## Creating Test Tables

In Jupyter or PySpark shell:

```python
# Create test database
spark.sql("CREATE DATABASE IF NOT EXISTS demo")

# Create accounts table
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.accounts (
        cons_acct_key BIGINT,
        rpt_as_of_mo STRING,
        base_ts TIMESTAMP,
        balance_am INT,
        actual_payment_am INT,
        credit_limit_am INT,
        past_due_am INT,
        days_past_due INT,
        asset_class_cd STRING
    )
    USING iceberg
    PARTITIONED BY (rpt_as_of_mo)
""")

# Create summary table
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.summary (
        cons_acct_key BIGINT,
        rpt_as_of_mo STRING,
        base_ts TIMESTAMP,
        balance_history ARRAY<INT>,
        payment_history ARRAY<INT>,
        credit_limit_history ARRAY<INT>,
        past_due_history ARRAY<INT>,
        days_past_due_history ARRAY<INT>,
        payment_rating_history ARRAY<STRING>,
        asset_class_history ARRAY<STRING>,
        payment_history_grid STRING
    )
    USING iceberg
    PARTITIONED BY (rpt_as_of_mo)
""")

# Create latest_summary table
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.latest_summary (
        cons_acct_key BIGINT,
        rpt_as_of_mo STRING,
        base_ts TIMESTAMP,
        balance_history ARRAY<INT>,
        payment_history ARRAY<INT>,
        credit_limit_history ARRAY<INT>,
        past_due_history ARRAY<INT>,
        days_past_due_history ARRAY<INT>,
        payment_rating_history ARRAY<STRING>,
        asset_class_history ARRAY<STRING>,
        payment_history_grid STRING
    )
    USING iceberg
""")
```

---

## Inserting Test Data

```python
# Insert test accounts
spark.sql("""
    INSERT INTO demo.accounts VALUES
    (1001, '2025-12', TIMESTAMP '2025-12-15 10:00:00', 5000, 500, 10000, 0, 0, 'A'),
    (1002, '2025-12', TIMESTAMP '2025-12-15 10:00:00', 7000, 700, 15000, 0, 0, 'A'),
    (1003, '2025-11', TIMESTAMP '2025-12-20 10:00:00', 4000, 400, 10000, 0, 0, 'A')
""")

# Verify
spark.sql("SELECT * FROM demo.accounts").show()
```

---

## Troubleshooting

### Services Not Starting
```bash
# Check logs
docker-compose logs minio
docker-compose logs rest
docker-compose logs spark-iceberg
```

### MinIO Bucket Not Created
```bash
# Manually create bucket
docker-compose exec mc mc mb minio/warehouse
```

### Spark Session Issues
```python
# In notebook, recreate session
spark.stop()

from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("v9_test") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "rest") \
    .config("spark.sql.catalog.demo.uri", "http://rest:8181") \
    .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/") \
    .getOrCreate()
```

### Clean Restart
```bash
docker-compose down -v
docker-compose up -d
```

---

## Environment Variables

| Variable | Value | Description |
|----------|-------|-------------|
| AWS_ACCESS_KEY_ID | admin | MinIO access key |
| AWS_SECRET_ACCESS_KEY | password | MinIO secret key |
| AWS_REGION | us-east-1 | AWS region |
| CATALOG_WAREHOUSE | s3://warehouse/ | Iceberg warehouse location |
