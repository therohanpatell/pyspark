# PySpark BigQuery Metrics Pipeline

A comprehensive data pipeline for processing business metrics using PySpark and BigQuery. This pipeline reads JSON configuration files from Google Cloud Storage (GCS), executes SQL queries with dynamic placeholder replacement, and writes results to BigQuery tables with precision handling and rollback capabilities.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [JSON Configuration Format](#json-configuration-format)
- [SQL Placeholders](#sql-placeholders)
- [Error Handling & Rollback](#error-handling--rollback)
- [API Reference](#api-reference)
- [Examples](#examples)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

## Architecture Overview

The pipeline consists of several key components:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   GCS Storage   │    │   PySpark       │    │   BigQuery      │
│   (JSON Config) │───▶│   Pipeline      │───▶│   Tables        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │   Partition     │
                       │   Metadata      │
                       └─────────────────┘
```

### Components:
1. **MetricsPipeline**: Main class handling metric processing
2. **Spark Session Management**: Managed context for Spark operations
3. **BigQuery Integration**: Direct write capabilities with schema alignment
4. **Dynamic SQL Processing**: Placeholder replacement system
5. **Error Handling & Rollback**: Transaction-safe operations

## Features

- ✅ **JSON Configuration-driven**: Define metrics in JSON format stored in GCS
- ✅ **Dynamic SQL Placeholders**: `{currently}` and `{partition_info}` replacement
- ✅ **Precision Handling**: Decimal precision preservation for financial metrics
- ✅ **Schema Alignment**: Automatic schema matching with BigQuery tables
- ✅ **Transaction Safety**: Rollback capabilities for failed operations
- ✅ **Overwrite Protection**: Handles existing metric overwrites gracefully
- ✅ **Dependency Management**: Process metrics in dependency order
- ✅ **Comprehensive Logging**: Detailed operation logging and error reporting
- ✅ **Validation**: Input validation for data integrity
- ✅ **Multiple Table Support**: Process metrics for different target tables

## Prerequisites

- Python 3.7+
- Apache Spark 3.0+
- Google Cloud SDK
- BigQuery API enabled
- Appropriate GCP permissions for BigQuery and GCS

### Required Python Packages

```bash
pip install pyspark google-cloud-bigquery google-cloud-storage
```

### Required Spark Packages

```bash
spark-submit --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.28.0
```

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd pyspark-bigquery-metrics-pipeline
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up Google Cloud credentials:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account-key.json"
```

## Configuration

### Environment Variables

```bash
# Google Cloud Project
export GCP_PROJECT="your-project-id"

# BigQuery Dataset
export BQ_DATASET="your-dataset"

# GCS Bucket
export GCS_BUCKET="your-bucket"

# Spark Configuration
export SPARK_HOME="/path/to/spark"
```

### BigQuery Table Schema

Your target BigQuery tables must have the following schema:

```sql
CREATE TABLE `project.dataset.metrics_table` (
    metric_id STRING NOT NULL,
    metric_name STRING NOT NULL,
    metric_type STRING NOT NULL,
    numerator_value NUMERIC,
    denominator_value NUMERIC,
    metric_output NUMERIC,
    business_data_date STRING NOT NULL,
    partition_dt STRING NOT NULL,
    pipeline_execution_ts TIMESTAMP NOT NULL
);
```

## Usage

### Basic Usage

```bash
python pyspark.py \
    --gcs_path "gs://your-bucket/config/metrics.json" \
    --run_date "2024-01-15" \
    --dependencies "daily_metrics,weekly_metrics" \
    --partition_info_table "project.dataset.partition_metadata"
```

### Spark Submit

```bash
spark-submit \
    --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.28.0 \
    pyspark.py \
    --gcs_path "gs://your-bucket/config/metrics.json" \
    --run_date "2024-01-15" \
    --dependencies "daily_metrics" \
    --partition_info_table "project.dataset.partition_metadata"
```

### Command Line Arguments

| Argument | Required | Description | Example |
|----------|----------|-------------|---------|
| `--gcs_path` | Yes | GCS path to JSON configuration file | `gs://bucket/config/metrics.json` |
| `--run_date` | Yes | Run date in YYYY-MM-DD format | `2024-01-15` |
| `--dependencies` | Yes | Comma-separated list of dependencies | `daily_metrics,weekly_metrics` |
| `--partition_info_table` | Yes | BigQuery table for partition metadata | `project.dataset.partition_info` |

## JSON Configuration Format

The JSON configuration file defines metrics to be processed:

```json
[
    {
        "metric_id": "daily_revenue",
        "metric_name": "Daily Revenue",
        "metric_type": "currency",
        "sql": "SELECT SUM(amount) as metric_output, COUNT(*) as numerator_value, 1 as denominator_value FROM `project.dataset.transactions` WHERE date = {currently}",
        "dependency": "daily_metrics",
        "target_table": "project.dataset.revenue_metrics"
    },
    {
        "metric_id": "conversion_rate",
        "metric_name": "Conversion Rate",
        "metric_type": "percentage",
        "sql": "SELECT converted_users/total_users as metric_output, converted_users as numerator_value, total_users as denominator_value FROM `project.dataset.user_stats` WHERE partition_dt = {partition_info}",
        "dependency": "daily_metrics",
        "target_table": "project.dataset.conversion_metrics"
    }
]
```

### Required Fields

- `metric_id`: Unique identifier for the metric
- `metric_name`: Human-readable name
- `metric_type`: Type of metric (currency, percentage, count, etc.)
- `sql`: SQL query with placeholders
- `dependency`: Dependency group for processing order
- `target_table`: BigQuery table in format `project.dataset.table`

## SQL Placeholders

The pipeline supports dynamic SQL placeholder replacement:

### `{currently}` Placeholder

Replaced with the `--run_date` parameter:

```sql
SELECT COUNT(*) as metric_output 
FROM `project.dataset.events` 
WHERE event_date = {currently}
```

Becomes:
```sql
SELECT COUNT(*) as metric_output 
FROM `project.dataset.events` 
WHERE event_date = '2024-01-15'
```

### `{partition_info}` Placeholder

Replaced with the latest partition date from metadata table:

```sql
SELECT AVG(value) as metric_output 
FROM `project.dataset.measurements` 
WHERE partition_dt = {partition_info}
```

The pipeline queries the partition metadata table to find the latest partition date for the referenced table.

## Error Handling & Rollback

### Automatic Rollback

The pipeline implements automatic rollback for failed operations:

1. **Transaction Tracking**: All processed metrics are tracked
2. **Rollback on Failure**: If any step fails, newly inserted metrics are rolled back
3. **Overwrite Protection**: Existing metrics that were overwritten cannot be automatically restored

### Error Types

| Error Type | Description | Rollback Behavior |
|------------|-------------|-------------------|
| `MetricsPipelineError` | Pipeline-specific errors | Full rollback of new metrics |
| `Validation Error` | JSON validation failures | No rollback needed |
| `SQL Error` | Query execution failures | Rollback processed metrics |
| `BigQuery Error` | Write operation failures | Rollback processed metrics |

## API Reference

### MetricsPipeline Class

The main pipeline class that orchestrates metric processing.

#### Constructor

```python
MetricsPipeline(spark: SparkSession, bq_client: bigquery.Client)
```

- `spark`: Initialized Spark session
- `bq_client`: BigQuery client instance

#### Key Methods

##### `read_json_from_gcs(gcs_path: str) -> List[Dict]`

Reads and parses JSON configuration from GCS.

**Parameters:**
- `gcs_path`: GCS path to JSON file

**Returns:** List of metric definitions

**Raises:** `MetricsPipelineError` if file cannot be read

##### `validate_json(json_data: List[Dict]) -> List[Dict]`

Validates JSON data for required fields and duplicates.

**Parameters:**
- `json_data`: List of metric definitions

**Returns:** Validated metric definitions

**Raises:** `MetricsPipelineError` if validation fails

##### `process_metrics(json_data: List[Dict], run_date: str, dependencies: List[str], partition_info_table: str) -> Dict[str, DataFrame]`

Processes metrics and creates Spark DataFrames.

**Parameters:**
- `json_data`: Validated metric definitions
- `run_date`: Run date for placeholder replacement
- `dependencies`: List of dependencies to process
- `partition_info_table`: Metadata table for partition info

**Returns:** Dictionary mapping target tables to DataFrames

##### `write_to_bq_with_overwrite(df: DataFrame, target_table: str) -> None`

Writes DataFrame to BigQuery with overwrite capability.

**Parameters:**
- `df`: Spark DataFrame to write
- `target_table`: Target BigQuery table

### Utility Functions

#### `managed_spark_session(app_name: str) -> SparkSession`

Context manager for Spark session with proper cleanup.

#### `validate_date_format(date_str: str) -> None`

Validates date format (YYYY-MM-DD).

#### `parse_arguments() -> argparse.Namespace`

Parses command line arguments.

## Examples

### Example 1: Basic Revenue Metrics

JSON Configuration:
```json
[
    {
        "metric_id": "daily_revenue",
        "metric_name": "Daily Revenue",
        "metric_type": "currency",
        "sql": "SELECT SUM(amount) as metric_output, COUNT(*) as numerator_value, 1 as denominator_value FROM `project.dataset.transactions` WHERE date = {currently}",
        "dependency": "daily_metrics",
        "target_table": "project.dataset.revenue_metrics"
    }
]
```

Command:
```bash
python pyspark.py \
    --gcs_path "gs://bucket/revenue_config.json" \
    --run_date "2024-01-15" \
    --dependencies "daily_metrics" \
    --partition_info_table "project.dataset.partition_info"
```

### Example 2: Conversion Rate with Partition Info

JSON Configuration:
```json
[
    {
        "metric_id": "conversion_rate",
        "metric_name": "Daily Conversion Rate",
        "metric_type": "percentage",
        "sql": "SELECT (converted_users * 100.0 / total_users) as metric_output, converted_users as numerator_value, total_users as denominator_value FROM `project.dataset.user_stats` WHERE partition_dt = {partition_info}",
        "dependency": "daily_metrics",
        "target_table": "project.dataset.conversion_metrics"
    }
]
```

### Example 3: Multiple Dependencies

```bash
python pyspark.py \
    --gcs_path "gs://bucket/multi_metrics.json" \
    --run_date "2024-01-15" \
    --dependencies "daily_metrics,weekly_metrics,monthly_metrics" \
    --partition_info_table "project.dataset.partition_info"
```

## Troubleshooting

### Common Issues

#### 1. GCS Path Not Found

**Error:** `GCS path inaccessible`

**Solution:** 
- Verify GCS path format: `gs://bucket/path/file.json`
- Check file exists and is readable
- Verify service account permissions

#### 2. BigQuery Table Not Found

**Error:** `Table not found`

**Solution:**
- Create target table with correct schema
- Verify table name format: `project.dataset.table`
- Check BigQuery permissions

#### 3. Placeholder Replacement Fails

**Error:** `Could not find table reference for {partition_info}`

**Solution:**
- Ensure SQL contains table references before placeholders
- Use backticks for table names: `` `project.dataset.table` ``
- Check partition metadata table exists

#### 4. Validation Errors

**Error:** `Missing required field 'metric_id'`

**Solution:**
- Verify JSON contains all required fields
- Check for empty or whitespace-only values
- Validate JSON syntax

### Debugging

Enable debug logging:
```python
import logging
logging.getLogger().setLevel(logging.DEBUG)
```

View DataFrame schemas:
```python
df.printSchema()
df.show(truncate=False)
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

### Code Style

- Follow PEP 8 guidelines
- Add docstrings for all functions
- Include type hints
- Write comprehensive tests

### Testing

Run tests with:
```bash
pytest tests/
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.