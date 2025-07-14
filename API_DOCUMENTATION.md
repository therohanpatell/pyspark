# API Documentation

This document provides detailed API documentation for the PySpark BigQuery Metrics Pipeline.

## Table of Contents

- [Classes](#classes)
  - [MetricsPipelineError](#metricspipelineerror)
  - [MetricsPipeline](#metricspipeline)
- [Functions](#functions)
  - [managed_spark_session](#managed_spark_session)
  - [parse_arguments](#parse_arguments)
  - [validate_date_format](#validate_date_format)
  - [main](#main)
- [Type Definitions](#type-definitions)
- [Usage Examples](#usage-examples)

## Classes

### MetricsPipelineError

Custom exception class for pipeline-specific errors.

```python
class MetricsPipelineError(Exception):
    """Custom exception for pipeline errors"""
    pass
```

**Usage:**
```python
raise MetricsPipelineError("Invalid configuration format")
```

### MetricsPipeline

Main pipeline class that orchestrates metric processing operations.

#### Constructor

```python
MetricsPipeline(spark: SparkSession, bq_client: bigquery.Client)
```

**Parameters:**
- `spark` (`SparkSession`): Initialized Spark session
- `bq_client` (`bigquery.Client`): BigQuery client instance

**Attributes:**
- `spark`: Spark session instance
- `bq_client`: BigQuery client instance
- `execution_id`: Unique execution identifier (UUID)
- `processed_metrics`: List of processed metric IDs for rollback
- `overwritten_metrics`: List of overwritten metric IDs for rollback
- `target_tables`: Set of target tables for rollback

**Example:**
```python
from pyspark.sql import SparkSession
from google.cloud import bigquery

spark = SparkSession.builder.appName("MetricsPipeline").getOrCreate()
bq_client = bigquery.Client()
pipeline = MetricsPipeline(spark, bq_client)
```

#### Methods

##### validate_gcs_path

```python
validate_gcs_path(gcs_path: str) -> str
```

Validates GCS path format and accessibility.

**Parameters:**
- `gcs_path` (`str`): GCS path to validate

**Returns:**
- `str`: Validated GCS path

**Raises:**
- `MetricsPipelineError`: If path is invalid or inaccessible

**Example:**
```python
validated_path = pipeline.validate_gcs_path("gs://bucket/config/metrics.json")
```

##### read_json_from_gcs

```python
read_json_from_gcs(gcs_path: str) -> List[Dict]
```

Reads JSON file from GCS and returns as list of dictionaries.

**Parameters:**
- `gcs_path` (`str`): GCS path to JSON file

**Returns:**
- `List[Dict]`: List of metric definitions

**Raises:**
- `MetricsPipelineError`: If file cannot be read or parsed

**Example:**
```python
json_data = pipeline.read_json_from_gcs("gs://bucket/config/metrics.json")
```

##### validate_json

```python
validate_json(json_data: List[Dict]) -> List[Dict]
```

Validates JSON data for required fields and duplicates.

**Parameters:**
- `json_data` (`List[Dict]`): List of metric definitions

**Returns:**
- `List[Dict]`: List of validated metric definitions

**Raises:**
- `MetricsPipelineError`: If validation fails

**Required Fields:**
- `metric_id`: Unique identifier
- `metric_name`: Human-readable name
- `metric_type`: Type of metric
- `sql`: SQL query with placeholders
- `dependency`: Dependency group
- `target_table`: BigQuery table (project.dataset.table format)

**Example:**
```python
validated_data = pipeline.validate_json(json_data)
```

##### find_placeholder_positions

```python
find_placeholder_positions(sql: str) -> List[Tuple[str, int, int]]
```

Finds all placeholders in SQL with their positions.

**Parameters:**
- `sql` (`str`): SQL query string

**Returns:**
- `List[Tuple[str, int, int]]`: List of tuples (placeholder_type, start_pos, end_pos)

**Supported Placeholders:**
- `{currently}`: Replaced with run date
- `{partition_info}`: Replaced with partition date from metadata

**Example:**
```python
placeholders = pipeline.find_placeholder_positions(
    "SELECT * FROM table WHERE date = {currently}"
)
# Returns: [('currently', 35, 46)]
```

##### get_table_for_placeholder

```python
get_table_for_placeholder(sql: str, placeholder_pos: int) -> Optional[Tuple[str, str]]
```

Finds the table associated with a placeholder based on its position.

**Parameters:**
- `sql` (`str`): SQL query string
- `placeholder_pos` (`int`): Position of the placeholder

**Returns:**
- `Optional[Tuple[str, str]]`: Tuple (dataset, table_name) or None if not found

**Example:**
```python
table_info = pipeline.get_table_for_placeholder(sql, 35)
# Returns: ('dataset', 'table_name')
```

##### get_partition_dt

```python
get_partition_dt(project_dataset: str, table_name: str, partition_info_table: str) -> Optional[str]
```

Gets latest partition_dt from metadata table.

**Parameters:**
- `project_dataset` (`str`): Dataset name
- `table_name` (`str`): Table name
- `partition_info_table` (`str`): Metadata table name

**Returns:**
- `Optional[str]`: Latest partition date as string or None

**Example:**
```python
partition_dt = pipeline.get_partition_dt(
    "dataset", "table", "project.dataset.partition_info"
)
```

##### replace_sql_placeholders

```python
replace_sql_placeholders(sql: str, run_date: str, partition_info_table: str) -> str
```

Replaces placeholders in SQL with appropriate dates.

**Parameters:**
- `sql` (`str`): SQL query string with placeholders
- `run_date` (`str`): CLI provided run date
- `partition_info_table` (`str`): Metadata table name

**Returns:**
- `str`: SQL with all placeholders replaced

**Example:**
```python
final_sql = pipeline.replace_sql_placeholders(
    "SELECT * FROM table WHERE date = {currently}",
    "2024-01-15",
    "project.dataset.partition_info"
)
```

##### normalize_numeric_value

```python
normalize_numeric_value(value: Union[int, float, Decimal, None]) -> Optional[str]
```

Normalizes numeric values to string representation to preserve precision.

**Parameters:**
- `value` (`Union[int, float, Decimal, None]`): Numeric value of any type

**Returns:**
- `Optional[str]`: String representation of the number or None

**Example:**
```python
normalized = pipeline.normalize_numeric_value(123.456)
# Returns: "123.456"
```

##### safe_decimal_conversion

```python
safe_decimal_conversion(value: Optional[str]) -> Optional[Decimal]
```

Safely converts string to Decimal for BigQuery.

**Parameters:**
- `value` (`Optional[str]`): String representation of number

**Returns:**
- `Optional[Decimal]`: Decimal value or None

**Example:**
```python
decimal_value = pipeline.safe_decimal_conversion("123.456")
```

##### check_dependencies_exist

```python
check_dependencies_exist(json_data: List[Dict], dependencies: List[str]) -> None
```

Checks if all specified dependencies exist in the JSON data.

**Parameters:**
- `json_data` (`List[Dict]`): List of metric definitions
- `dependencies` (`List[str]`): List of dependencies to check

**Raises:**
- `MetricsPipelineError`: If any dependency is missing

**Example:**
```python
pipeline.check_dependencies_exist(json_data, ["daily_metrics", "weekly_metrics"])
```

##### execute_sql

```python
execute_sql(sql: str, run_date: str, partition_info_table: str, metric_id: Optional[str] = None) -> Dict
```

Executes SQL query with dynamic placeholder replacement.

**Parameters:**
- `sql` (`str`): SQL query string with placeholders
- `run_date` (`str`): CLI provided run date
- `partition_info_table` (`str`): Metadata table name
- `metric_id` (`Optional[str]`): Optional metric ID for error reporting

**Returns:**
- `Dict`: Dictionary with query results containing:
  - `metric_output`: Main metric value
  - `numerator_value`: Numerator for ratio metrics
  - `denominator_value`: Denominator for ratio metrics
  - `business_data_date`: Business date (run_date - 1)

**Example:**
```python
result = pipeline.execute_sql(
    "SELECT COUNT(*) as metric_output, 1 as numerator_value, 1 as denominator_value FROM table",
    "2024-01-15",
    "project.dataset.partition_info",
    "daily_count"
)
```

##### rollback_metric

```python
rollback_metric(metric_id: str, target_table: str, partition_dt: str) -> None
```

Rolls back a specific metric from the target table.

**Parameters:**
- `metric_id` (`str`): Metric ID to rollback
- `target_table` (`str`): Target BigQuery table
- `partition_dt` (`str`): Partition date for the metric

**Example:**
```python
pipeline.rollback_metric("daily_count", "project.dataset.metrics", "2024-01-15")
```

##### rollback_processed_metrics

```python
rollback_processed_metrics(target_table: str, partition_dt: str) -> None
```

Rolls back all processed metrics in case of failure.

**Parameters:**
- `target_table` (`str`): Target BigQuery table
- `partition_dt` (`str`): Partition date for rollback

**Note:** Only rolls back newly inserted metrics, not overwritten ones.

**Example:**
```python
pipeline.rollback_processed_metrics("project.dataset.metrics", "2024-01-15")
```

##### process_metrics

```python
process_metrics(json_data: List[Dict], run_date: str, dependencies: List[str], partition_info_table: str) -> Dict[str, DataFrame]
```

Processes metrics and creates Spark DataFrames grouped by target_table.

**Parameters:**
- `json_data` (`List[Dict]`): List of metric definitions
- `run_date` (`str`): CLI provided run date
- `dependencies` (`List[str]`): List of dependencies to process
- `partition_info_table` (`str`): Metadata table name

**Returns:**
- `Dict[str, DataFrame]`: Dictionary mapping target_table to Spark DataFrame

**Example:**
```python
metrics_dfs = pipeline.process_metrics(
    json_data,
    "2024-01-15",
    ["daily_metrics"],
    "project.dataset.partition_info"
)
```

##### get_bq_table_schema

```python
get_bq_table_schema(table_name: str) -> List[bigquery.SchemaField]
```

Gets BigQuery table schema.

**Parameters:**
- `table_name` (`str`): Full table name (project.dataset.table)

**Returns:**
- `List[bigquery.SchemaField]`: List of schema fields

**Raises:**
- `MetricsPipelineError`: If table not found

**Example:**
```python
schema = pipeline.get_bq_table_schema("project.dataset.metrics")
```

##### align_schema_with_bq

```python
align_schema_with_bq(df: DataFrame, target_table: str) -> DataFrame
```

Aligns Spark DataFrame with BigQuery table schema.

**Parameters:**
- `df` (`DataFrame`): Spark DataFrame
- `target_table` (`str`): BigQuery table name

**Returns:**
- `DataFrame`: Schema-aligned DataFrame

**Example:**
```python
aligned_df = pipeline.align_schema_with_bq(df, "project.dataset.metrics")
```

##### write_to_bq

```python
write_to_bq(df: DataFrame, target_table: str) -> None
```

Writes DataFrame to BigQuery table with transaction safety.

**Parameters:**
- `df` (`DataFrame`): Spark DataFrame to write
- `target_table` (`str`): Target BigQuery table

**Example:**
```python
pipeline.write_to_bq(df, "project.dataset.metrics")
```

##### check_existing_metrics

```python
check_existing_metrics(metric_ids: List[str], partition_dt: str, target_table: str) -> List[str]
```

Checks which metric IDs already exist in BigQuery table.

**Parameters:**
- `metric_ids` (`List[str]`): List of metric IDs to check
- `partition_dt` (`str`): Partition date to check
- `target_table` (`str`): Target BigQuery table

**Returns:**
- `List[str]`: List of existing metric IDs

**Example:**
```python
existing = pipeline.check_existing_metrics(
    ["metric1", "metric2"],
    "2024-01-15",
    "project.dataset.metrics"
)
```

##### delete_existing_metrics

```python
delete_existing_metrics(metric_ids: List[str], partition_dt: str, target_table: str) -> None
```

Deletes existing metrics from BigQuery table.

**Parameters:**
- `metric_ids` (`List[str]`): List of metric IDs to delete
- `partition_dt` (`str`): Partition date for deletion
- `target_table` (`str`): Target BigQuery table

**Example:**
```python
pipeline.delete_existing_metrics(
    ["metric1", "metric2"],
    "2024-01-15",
    "project.dataset.metrics"
)
```

##### write_to_bq_with_overwrite

```python
write_to_bq_with_overwrite(df: DataFrame, target_table: str) -> None
```

Writes DataFrame to BigQuery table with overwrite capability.

**Parameters:**
- `df` (`DataFrame`): Spark DataFrame to write
- `target_table` (`str`): Target BigQuery table

**Example:**
```python
pipeline.write_to_bq_with_overwrite(df, "project.dataset.metrics")
```

##### rollback_all_processed_metrics

```python
rollback_all_processed_metrics(partition_dt: str) -> None
```

Rolls back all processed metrics from all target tables.

**Parameters:**
- `partition_dt` (`str`): Partition date for rollback

**Example:**
```python
pipeline.rollback_all_processed_metrics("2024-01-15")
```

## Functions

### managed_spark_session

```python
@contextmanager
def managed_spark_session(app_name: str = "MetricsPipeline") -> SparkSession
```

Context manager for Spark session with proper cleanup.

**Parameters:**
- `app_name` (`str`): Spark application name (default: "MetricsPipeline")

**Yields:**
- `SparkSession`: Spark session instance

**Example:**
```python
with managed_spark_session("MyApp") as spark:
    # Use spark session
    df = spark.read.json("path/to/file.json")
```

### parse_arguments

```python
def parse_arguments() -> argparse.Namespace
```

Parses command line arguments.

**Returns:**
- `argparse.Namespace`: Parsed arguments containing:
  - `gcs_path`: GCS path to JSON input file
  - `run_date`: Run date in YYYY-MM-DD format
  - `dependencies`: Comma-separated list of dependencies
  - `partition_info_table`: BigQuery table for partition info

**Example:**
```python
args = parse_arguments()
print(f"GCS Path: {args.gcs_path}")
print(f"Run Date: {args.run_date}")
```

### validate_date_format

```python
def validate_date_format(date_str: str) -> None
```

Validates date format (YYYY-MM-DD).

**Parameters:**
- `date_str` (`str`): Date string to validate

**Raises:**
- `MetricsPipelineError`: If date format is invalid

**Example:**
```python
validate_date_format("2024-01-15")  # Valid
validate_date_format("01/15/2024")  # Raises MetricsPipelineError
```

### main

```python
def main() -> None
```

Main function with comprehensive error handling and resource management.

**Features:**
- Argument parsing and validation
- Spark session management
- BigQuery client initialization
- Pipeline execution with rollback on failure
- Comprehensive logging and error reporting

**Example:**
```python
if __name__ == "__main__":
    main()
```

## Type Definitions

### Common Types

```python
from typing import Dict, List, Optional, Tuple, Union
from decimal import Decimal
from pyspark.sql import DataFrame, SparkSession
from google.cloud import bigquery
```

### Metric Definition

```python
MetricDefinition = Dict[str, str]
# Example:
# {
#     "metric_id": "daily_revenue",
#     "metric_name": "Daily Revenue",
#     "metric_type": "currency",
#     "sql": "SELECT SUM(amount) as metric_output FROM transactions WHERE date = {currently}",
#     "dependency": "daily_metrics",
#     "target_table": "project.dataset.revenue_metrics"
# }
```

### SQL Result

```python
SqlResult = Dict[str, Union[str, Decimal, None]]
# Example:
# {
#     "metric_output": "1234.56",
#     "numerator_value": "1234",
#     "denominator_value": "1",
#     "business_data_date": "2024-01-14"
# }
```

## Usage Examples

### Basic Pipeline Usage

```python
from pyspark.sql import SparkSession
from google.cloud import bigquery
from pipeline import MetricsPipeline

# Initialize components
spark = SparkSession.builder.appName("MetricsPipeline").getOrCreate()
bq_client = bigquery.Client()
pipeline = MetricsPipeline(spark, bq_client)

# Read and process metrics
json_data = pipeline.read_json_from_gcs("gs://bucket/config.json")
validated_data = pipeline.validate_json(json_data)
metrics_dfs = pipeline.process_metrics(
    validated_data,
    "2024-01-15",
    ["daily_metrics"],
    "project.dataset.partition_info"
)

# Write to BigQuery
for target_table, df in metrics_dfs.items():
    aligned_df = pipeline.align_schema_with_bq(df, target_table)
    pipeline.write_to_bq_with_overwrite(aligned_df, target_table)
```

### Error Handling Example

```python
try:
    pipeline = MetricsPipeline(spark, bq_client)
    json_data = pipeline.read_json_from_gcs("gs://bucket/config.json")
    validated_data = pipeline.validate_json(json_data)
    # ... process metrics
except MetricsPipelineError as e:
    logger.error(f"Pipeline error: {e}")
    # Rollback if needed
    pipeline.rollback_all_processed_metrics("2024-01-15")
```

### Custom Validation Example

```python
def validate_custom_metrics(json_data: List[Dict]) -> List[Dict]:
    """Custom validation for business rules"""
    for record in json_data:
        # Custom validation logic
        if record['metric_type'] == 'currency':
            if 'SUM' not in record['sql'].upper():
                raise MetricsPipelineError(
                    f"Currency metrics must use SUM aggregation: {record['metric_id']}"
                )
    return json_data

# Use in pipeline
validated_data = pipeline.validate_json(json_data)
custom_validated = validate_custom_metrics(validated_data)
```

This API documentation provides comprehensive details about all classes, methods, and functions in the pipeline, along with practical usage examples.