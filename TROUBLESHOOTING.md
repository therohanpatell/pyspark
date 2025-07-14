# Troubleshooting Guide

This guide covers common issues, error messages, and solutions for the PySpark BigQuery Metrics Pipeline.

## Table of Contents

- [Common Issues](#common-issues)
- [Error Messages](#error-messages)
- [Debugging Techniques](#debugging-techniques)
- [Performance Issues](#performance-issues)
- [Configuration Problems](#configuration-problems)
- [Environment Setup](#environment-setup)
- [Monitoring and Logging](#monitoring-and-logging)

## Common Issues

### 1. GCS Path Issues

#### Problem: GCS path not found or inaccessible
```
ERROR: GCS path inaccessible: gs://bucket/config/metrics.json
```

**Solutions:**
- ✅ Verify the GCS path format: `gs://bucket/path/file.json`
- ✅ Check if the file exists in the bucket
- ✅ Ensure service account has Storage Object Viewer permissions
- ✅ Verify `GOOGLE_APPLICATION_CREDENTIALS` environment variable is set

**Testing:**
```bash
# Test GCS access
gsutil ls gs://your-bucket/config/
gsutil cat gs://your-bucket/config/metrics.json | head -n 10
```

#### Problem: JSON file format issues
```
ERROR: Failed to read JSON from GCS: Invalid JSON format
```

**Solutions:**
- ✅ Validate JSON syntax using online validators or `jq`
- ✅ Check for trailing commas in JSON
- ✅ Ensure proper UTF-8 encoding
- ✅ Verify multiline JSON is properly formatted

**Testing:**
```bash
# Validate JSON syntax
gsutil cat gs://your-bucket/config/metrics.json | jq '.'
```

### 2. BigQuery Issues

#### Problem: Table not found
```
ERROR: Table not found: project.dataset.metrics_table
```

**Solutions:**
- ✅ Verify table name format: `project.dataset.table`
- ✅ Check if the table exists in BigQuery
- ✅ Ensure service account has BigQuery Data Editor permissions
- ✅ Verify project ID is correct

**Testing:**
```bash
# Check if table exists
bq show project:dataset.table_name
```

#### Problem: Schema mismatch
```
ERROR: Schema mismatch between DataFrame and BigQuery table
```

**Solutions:**
- ✅ Compare DataFrame schema with BigQuery table schema
- ✅ Check column names and types
- ✅ Ensure BigQuery table has correct schema (see README for schema)
- ✅ Use `align_schema_with_bq()` method

**Testing:**
```python
# Check DataFrame schema
df.printSchema()

# Check BigQuery table schema
bq_schema = pipeline.get_bq_table_schema("project.dataset.table")
for field in bq_schema:
    print(f"{field.name}: {field.field_type}")
```

### 3. Spark Issues

#### Problem: Spark session fails to start
```
ERROR: Could not create Spark session
```

**Solutions:**
- ✅ Check if Spark is properly installed
- ✅ Verify `SPARK_HOME` environment variable
- ✅ Ensure sufficient memory for Spark driver
- ✅ Check for port conflicts

**Testing:**
```bash
# Check Spark installation
$SPARK_HOME/bin/spark-submit --version

# Test Spark session
$SPARK_HOME/bin/pyspark --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.28.0
```

#### Problem: BigQuery connector not found
```
ERROR: java.lang.ClassNotFoundException: com.google.cloud.spark.bigquery
```

**Solutions:**
- ✅ Include BigQuery connector in spark-submit command
- ✅ Check connector version compatibility
- ✅ Verify internet connectivity for downloading packages

**Command:**
```bash
spark-submit --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.28.0 pyspark.py
```

### 4. SQL and Placeholder Issues

#### Problem: Placeholder not replaced
```
ERROR: Could not find table reference for {partition_info} placeholder
```

**Solutions:**
- ✅ Ensure SQL contains table references before placeholders
- ✅ Use backticks for table names: `` `project.dataset.table` ``
- ✅ Check if partition metadata table exists and has data
- ✅ Verify table reference format

**Example:**
```sql
-- ❌ Wrong: placeholder without table reference
SELECT * FROM somewhere WHERE date = {partition_info}

-- ✅ Correct: table reference before placeholder
SELECT * FROM `project.dataset.table` WHERE partition_dt = {partition_info}
```

#### Problem: Partition info not found
```
ERROR: Could not determine partition_dt for table dataset.table
```

**Solutions:**
- ✅ Check if partition metadata table exists
- ✅ Verify partition metadata table has data for the referenced table
- ✅ Ensure partition metadata table format is correct
- ✅ Check table name extraction logic

**Testing:**
```sql
-- Check partition metadata
SELECT * FROM `project.dataset.partition_info` 
WHERE project_dataset = 'dataset' AND table_name = 'table_name'
ORDER BY partition_dt DESC LIMIT 5;
```

### 5. Data Validation Issues

#### Problem: Missing required fields
```
ERROR: Record 0: Missing required field 'metric_id'
```

**Solutions:**
- ✅ Verify all required fields are present in JSON
- ✅ Check for null or empty values
- ✅ Ensure field names match exactly
- ✅ Validate JSON structure

**Required fields:**
- `metric_id`
- `metric_name`
- `metric_type`
- `sql`
- `dependency`
- `target_table`

#### Problem: Duplicate metric IDs
```
ERROR: Duplicate metric_id 'daily_revenue' found
```

**Solutions:**
- ✅ Ensure all metric IDs are unique
- ✅ Check for case sensitivity issues
- ✅ Validate JSON for duplicate entries

### 6. Numeric Precision Issues

#### Problem: Precision loss in calculations
```
WARNING: Very small denominator value detected
```

**Solutions:**
- ✅ Use `Decimal` type for high precision calculations
- ✅ Check for division by zero or very small numbers
- ✅ Verify numeric field types in BigQuery schema
- ✅ Use appropriate precision settings

**Example:**
```python
# Use Decimal for precision
from decimal import Decimal
value = Decimal('123.456789')
```

## Error Messages

### Pipeline Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `MetricsPipelineError: Invalid GCS path format` | GCS path doesn't start with `gs://` | Use correct format: `gs://bucket/path/file.json` |
| `MetricsPipelineError: No data found in JSON file` | JSON file is empty | Check file contents and format |
| `MetricsPipelineError: Missing dependencies in JSON data` | Specified dependency not found | Verify dependency names in JSON |
| `MetricsPipelineError: Invalid denominator value: denominator_value is 0` | Division by zero | Check SQL logic for denominator calculation |
| `MetricsPipelineError: Failed to execute SQL` | SQL syntax or execution error | Validate SQL query and table references |

### BigQuery Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `google.cloud.exceptions.NotFound: Table not found` | Table doesn't exist | Create table or check table name |
| `google.cloud.exceptions.Forbidden: Access denied` | Insufficient permissions | Grant appropriate BigQuery permissions |
| `google.cloud.exceptions.BadRequest: Invalid query` | SQL syntax error | Validate SQL query syntax |

### Spark Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `pyspark.sql.utils.AnalysisException: Table or view not found` | Table reference error | Check table names and existence |
| `py4j.protocol.Py4JJavaError: An error occurred while calling` | Java/Spark runtime error | Check Spark logs for detailed error |

## Debugging Techniques

### 1. Enable Debug Logging

```python
import logging

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Enable Spark SQL logging
spark.sparkContext.setLogLevel("DEBUG")
```

### 2. Inspect DataFrames

```python
# Check DataFrame schema
df.printSchema()

# Show DataFrame contents
df.show(20, truncate=False)

# Count records
print(f"Record count: {df.count()}")

# Check for null values
df.select([col(c).isNull().alias(c) for c in df.columns]).show()
```

### 3. Validate SQL Queries

```python
# Test SQL query directly in BigQuery
query = """
SELECT COUNT(*) as metric_output, 
       1 as numerator_value, 
       1 as denominator_value 
FROM `project.dataset.table` 
WHERE date = '2024-01-15'
"""

# Test with BigQuery client
query_job = bq_client.query(query)
results = query_job.result()
for row in results:
    print(dict(row))
```

### 4. Step-by-Step Debugging

```python
try:
    # Step 1: Read JSON
    json_data = pipeline.read_json_from_gcs(gcs_path)
    print(f"✅ Read {len(json_data)} records from JSON")
    
    # Step 2: Validate JSON
    validated_data = pipeline.validate_json(json_data)
    print(f"✅ Validated {len(validated_data)} records")
    
    # Step 3: Process each metric individually
    for record in validated_data:
        print(f"Processing metric: {record['metric_id']}")
        
        # Test SQL execution
        sql_result = pipeline.execute_sql(
            record['sql'], 
            run_date, 
            partition_info_table,
            record['metric_id']
        )
        print(f"Result: {sql_result}")
        
except Exception as e:
    print(f"Error at step: {e}")
    import traceback
    traceback.print_exc()
```

### 5. Check Placeholder Replacement

```python
# Test placeholder replacement
sql = "SELECT * FROM `project.dataset.table` WHERE date = {currently}"
final_sql = pipeline.replace_sql_placeholders(sql, "2024-01-15", partition_info_table)
print(f"Original SQL: {sql}")
print(f"Final SQL: {final_sql}")
```

## Performance Issues

### 1. Slow GCS Reads

**Symptoms:**
- Long delays when reading JSON from GCS
- Timeouts during file access

**Solutions:**
- ✅ Use smaller JSON files or split large configurations
- ✅ Optimize GCS bucket location (same region as Spark cluster)
- ✅ Enable parallel reads for large files
- ✅ Use caching for frequently accessed files

### 2. BigQuery Query Performance

**Symptoms:**
- Slow SQL execution
- Query timeouts

**Solutions:**
- ✅ Optimize SQL queries (use indexes, partitioning)
- ✅ Limit data scanned with WHERE clauses
- ✅ Use query caching
- ✅ Consider materialized views for complex queries

### 3. Spark Performance

**Symptoms:**
- Slow DataFrame operations
- Memory issues

**Solutions:**
- ✅ Increase Spark driver and executor memory
- ✅ Optimize partition sizes
- ✅ Use appropriate DataFrame operations
- ✅ Enable adaptive query execution

```python
# Spark configuration for better performance
spark = SparkSession.builder \
    .appName("MetricsPipeline") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()
```

## Configuration Problems

### 1. Environment Variables

**Check required environment variables:**
```bash
echo $GOOGLE_APPLICATION_CREDENTIALS
echo $SPARK_HOME
echo $GCP_PROJECT
```

### 2. Service Account Permissions

**Required permissions:**
- BigQuery Data Editor
- BigQuery Job User
- Storage Object Viewer
- Storage Object Creator (if creating temp files)

### 3. Network Configuration

**Check network connectivity:**
```bash
# Test BigQuery API
curl -H "Authorization: Bearer $(gcloud auth print-access-token)" \
     https://bigquery.googleapis.com/bigquery/v2/projects/your-project/datasets

# Test GCS API
curl -H "Authorization: Bearer $(gcloud auth print-access-token)" \
     https://storage.googleapis.com/storage/v1/b/your-bucket/o
```

## Environment Setup

### 1. Python Environment

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Verify installations
python -c "import pyspark; print(pyspark.__version__)"
python -c "from google.cloud import bigquery; print('BigQuery client OK')"
```

### 2. Spark Configuration

```bash
# Set environment variables
export SPARK_HOME=/path/to/spark
export JAVA_HOME=/path/to/java

# Test Spark
$SPARK_HOME/bin/spark-submit --version
```

### 3. Google Cloud Setup

```bash
# Install gcloud CLI
curl https://sdk.cloud.google.com | bash

# Authenticate
gcloud auth login
gcloud auth application-default login

# Set project
gcloud config set project your-project-id
```

## Monitoring and Logging

### 1. Log Analysis

**Common log patterns to look for:**
```
ERROR - MetricsPipelineError: [error message]
WARNING - [warning message]
INFO - Successfully processed [number] metrics
```

### 2. Monitoring Metrics

**Key metrics to monitor:**
- Processing time per metric
- Success/failure rates
- Memory usage
- BigQuery slot usage
- GCS read latency

### 3. Alerting

**Set up alerts for:**
- Pipeline failures
- Unusual processing times
- Data quality issues
- Resource usage thresholds

### 4. Health Checks

```python
def health_check():
    """Basic health check for pipeline dependencies"""
    checks = {}
    
    # Check BigQuery connectivity
    try:
        bq_client = bigquery.Client()
        bq_client.query("SELECT 1").result()
        checks['bigquery'] = 'OK'
    except Exception as e:
        checks['bigquery'] = f'ERROR: {e}'
    
    # Check GCS connectivity
    try:
        from google.cloud import storage
        storage_client = storage.Client()
        storage_client.list_buckets()
        checks['gcs'] = 'OK'
    except Exception as e:
        checks['gcs'] = f'ERROR: {e}'
    
    # Check Spark
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark:
            checks['spark'] = 'OK'
        else:
            checks['spark'] = 'No active session'
    except Exception as e:
        checks['spark'] = f'ERROR: {e}'
    
    return checks
```

## Getting Help

### 1. Log Collection

When reporting issues, collect:
- Full error messages and stack traces
- Pipeline configuration (JSON)
- Spark logs
- BigQuery job IDs
- Environment details

### 2. Debugging Information

```python
# Collect debugging info
def collect_debug_info():
    import platform
    import pyspark
    
    info = {
        'python_version': platform.python_version(),
        'pyspark_version': pyspark.__version__,
        'platform': platform.platform(),
        'java_version': os.environ.get('JAVA_VERSION', 'Unknown'),
        'spark_home': os.environ.get('SPARK_HOME', 'Not set')
    }
    
    return info
```

### 3. Support Resources

- **Documentation**: README.md, API_DOCUMENTATION.md
- **Examples**: examples/ directory
- **Issues**: GitHub issues (if applicable)
- **Community**: Stack Overflow with tags `pyspark`, `bigquery`

Remember to never share sensitive information like service account keys, project IDs, or actual data when seeking help.