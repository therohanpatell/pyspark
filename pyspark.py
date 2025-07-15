import argparse
import logging
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union
import sys
from decimal import Decimal
import os
import tempfile
from contextlib import contextmanager
import uuid
import time
import threading
from functools import wraps
import signal
import psutil
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_date, 
    when, isnan, isnull, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    TimestampType, DecimalType, IntegerType, DoubleType
)

from google.cloud import bigquery
from google.cloud.exceptions import NotFound, GoogleCloudError, Forbidden, BadRequest
from google.api_core.exceptions import RetryError, DeadlineExceeded
from google.api_core import retry

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('pipeline.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)

# Global variable to track shutdown state
shutdown_requested = False

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_requested
    shutdown_requested = True
    logger.warning(f"Received signal {signum}, initiating graceful shutdown...")

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

class MetricsPipelineError(Exception):
    """Custom exception for pipeline errors"""
    def __init__(self, message: str, error_code: str = None, context: Dict = None):
        super().__init__(message)
        self.error_code = error_code or "PIPELINE_ERROR"
        self.context = context or {}
        self.timestamp = datetime.utcnow().isoformat()

class RetryableError(MetricsPipelineError):
    """Exception for errors that can be retried"""
    pass

class NonRetryableError(MetricsPipelineError):
    """Exception for errors that should not be retried"""
    pass

class ResourceExhaustionError(MetricsPipelineError):
    """Exception for resource exhaustion scenarios"""
    pass

def retry_on_failure(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0, exceptions: Tuple = None):
    """
    Decorator for retrying operations with exponential backoff
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Backoff multiplier for delay
        exceptions: Tuple of exception types to catch and retry
    """
    if exceptions is None:
        exceptions = (RetryableError, GoogleCloudError, ConnectionError, TimeoutError)
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_retries + 1):
                if shutdown_requested:
                    raise MetricsPipelineError("Pipeline shutdown requested")
                
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_retries:
                        logger.error(f"Function {func.__name__} failed after {max_retries} retries: {str(e)}")
                        break
                    
                    logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {str(e)}. Retrying in {current_delay}s...")
                    time.sleep(current_delay)
                    current_delay *= backoff
                except NonRetryableError as e:
                    logger.error(f"Non-retryable error in {func.__name__}: {str(e)}")
                    raise
                except Exception as e:
                    logger.error(f"Unexpected error in {func.__name__}: {str(e)}")
                    raise
            
            # If we get here, all retries failed
            raise MetricsPipelineError(f"Function {func.__name__} failed after {max_retries} retries", context={"last_error": str(last_exception)})
        
        return wrapper
    return decorator

def monitor_resources():
    """Monitor system resources and warn if thresholds are exceeded"""
    try:
        # Check memory usage
        memory = psutil.virtual_memory()
        if memory.percent > 85:
            logger.warning(f"High memory usage: {memory.percent}%")
            if memory.percent > 95:
                raise ResourceExhaustionError(f"Memory usage critically high: {memory.percent}%")
        
        # Check disk usage
        disk = psutil.disk_usage('/')
        if disk.percent > 90:
            logger.warning(f"High disk usage: {disk.percent}%")
            if disk.percent > 95:
                raise ResourceExhaustionError(f"Disk usage critically high: {disk.percent}%")
        
        # Check CPU usage (average over 1 second)
        cpu_percent = psutil.cpu_percent(interval=1)
        if cpu_percent > 90:
            logger.warning(f"High CPU usage: {cpu_percent}%")
            
    except Exception as e:
        logger.warning(f"Failed to monitor resources: {str(e)}")

def validate_environment():
    """Validate required environment variables and configuration"""
    required_env_vars = [
        'GOOGLE_APPLICATION_CREDENTIALS',
        'GOOGLE_CLOUD_PROJECT'
    ]
    
    missing_vars = []
    for var in required_env_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        raise NonRetryableError(f"Missing required environment variables: {missing_vars}")
    
    # Validate Google Cloud credentials file exists
    creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if not Path(creds_path).exists():
        raise NonRetryableError(f"Google Cloud credentials file not found: {creds_path}")
    
    logger.info("Environment validation passed")

class MetricsPipeline:
    """Main pipeline class for processing metrics"""
    
    def __init__(self, spark: SparkSession, bq_client: bigquery.Client):
        self.spark = spark
        self.bq_client = bq_client
        self.execution_id = str(uuid.uuid4())
        self.processed_metrics = []  # Track processed metrics for rollback
        self.overwritten_metrics = []  # Track overwritten metrics for rollback
        self.target_tables = set()  # Track target tables for rollback
        self.temp_files = []  # Track temporary files for cleanup
        self.start_time = datetime.utcnow()
        self.lock = threading.Lock()  # Thread safety for shared state
        
        # Performance monitoring
        self.metrics_processed = 0
        self.errors_encountered = 0
        
        logger.info(f"Pipeline initialized with execution_id: {self.execution_id}")
        
    def cleanup_temp_files(self):
        """Clean up temporary files"""
        with self.lock:
            for temp_file in self.temp_files:
                try:
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                        logger.debug(f"Removed temporary file: {temp_file}")
                except Exception as e:
                    logger.warning(f"Failed to remove temporary file {temp_file}: {str(e)}")
            self.temp_files.clear()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup_temp_files()
        
    @retry_on_failure(max_retries=2, delay=2.0)
    def validate_gcs_path(self, gcs_path: str) -> str:
        """
        Validate GCS path format and accessibility with enhanced error handling
        
        Args:
            gcs_path: GCS path to validate
            
        Returns:
            Validated GCS path
            
        Raises:
            MetricsPipelineError: If path is invalid or inaccessible
        """
        if not gcs_path:
            raise NonRetryableError("GCS path cannot be empty")
        
        # Check basic format
        if not gcs_path.startswith('gs://'):
            raise NonRetryableError(f"Invalid GCS path format: {gcs_path}. Must start with 'gs://'")
        
        # Check path structure
        path_parts = gcs_path.replace('gs://', '').split('/')
        if len(path_parts) < 2:
            raise NonRetryableError(f"Invalid GCS path structure: {gcs_path}. Must include bucket and object path")
        
        bucket_name = path_parts[0]
        if not bucket_name:
            raise NonRetryableError(f"Invalid GCS path: bucket name cannot be empty in {gcs_path}")
        
        # Enhanced validation for object path
        object_path = '/'.join(path_parts[1:])
        if not object_path:
            raise NonRetryableError(f"Invalid GCS path: object path cannot be empty in {gcs_path}")
        
        # Check for invalid characters
        invalid_chars = ['<', '>', ':', '"', '|', '?', '*']
        for char in invalid_chars:
            if char in gcs_path:
                raise NonRetryableError(f"Invalid character '{char}' in GCS path: {gcs_path}")
        
        # Test accessibility by attempting to read file info
        try:
            monitor_resources()  # Check resources before operation
            
            # Try to read just the schema/structure without loading data
            test_df = self.spark.read.option("multiline", "true").json(gcs_path).limit(0)
            
            # Verify the file has the expected structure
            if not test_df.columns:
                raise RetryableError(f"GCS file appears to be empty or has no readable structure: {gcs_path}")
            
            logger.info(f"GCS path validated successfully: {gcs_path}")
            return gcs_path
            
        except Exception as e:
            error_msg = f"GCS path inaccessible: {gcs_path}. Error: {str(e)}"
            
            # Categorize the error
            if "java.io.FileNotFoundException" in str(e) or "No such file or directory" in str(e):
                raise NonRetryableError(error_msg)
            elif "Access Denied" in str(e) or "Permission denied" in str(e):
                raise NonRetryableError(error_msg)
            elif "timeout" in str(e).lower() or "deadline" in str(e).lower():
                raise RetryableError(error_msg)
            else:
                raise RetryableError(error_msg)
    
    @retry_on_failure(max_retries=3, delay=1.0)
    def read_json_from_gcs(self, gcs_path: str) -> List[Dict]:
        """
        Read JSON file from GCS and return as list of dictionaries with enhanced error handling
        
        Args:
            gcs_path: GCS path to JSON file
            
        Returns:
            List of metric definitions
            
        Raises:
            MetricsPipelineError: If file cannot be read or parsed
        """
        try:
            # Validate GCS path first
            validated_path = self.validate_gcs_path(gcs_path)
            
            logger.info(f"Reading JSON from GCS: {validated_path}")
            monitor_resources()
            
            # Read JSON file using Spark with enhanced options
            try:
                df = self.spark.read \
                    .option("multiline", "true") \
                    .option("mode", "PERMISSIVE") \
                    .option("columnNameOfCorruptRecord", "_corrupt_record") \
                    .json(validated_path)
                    
            except Exception as e:
                if "java.io.FileNotFoundException" in str(e):
                    raise NonRetryableError(f"JSON file not found: {validated_path}")
                elif "java.lang.OutOfMemoryError" in str(e):
                    raise ResourceExhaustionError(f"Out of memory reading JSON file: {validated_path}")
                else:
                    raise RetryableError(f"Failed to read JSON file: {validated_path}. Error: {str(e)}")
            
            # Check for corrupt records
            if "_corrupt_record" in df.columns:
                corrupt_records = df.filter(col("_corrupt_record").isNotNull()).count()
                if corrupt_records > 0:
                    logger.warning(f"Found {corrupt_records} corrupt records in JSON file")
                    # Show sample corrupt records for debugging
                    df.filter(col("_corrupt_record").isNotNull()).select("_corrupt_record").show(5, truncate=False)
            
            record_count = df.count()
            if record_count == 0:
                raise NonRetryableError(f"No valid data found in JSON file: {validated_path}")
            
            # Validate data size before collecting
            if record_count > 10000:  # Configurable threshold
                logger.warning(f"Large JSON file detected: {record_count} records. Consider processing in batches.")
            
            # Convert to list of dictionaries with memory management
            try:
                json_data = [row.asDict() for row in df.collect()]
            except Exception as e:
                if "java.lang.OutOfMemoryError" in str(e):
                    raise ResourceExhaustionError(f"Out of memory processing JSON data. File too large: {record_count} records")
                else:
                    raise RetryableError(f"Failed to process JSON data: {str(e)}")
            
            logger.info(f"Successfully read {len(json_data)} records from JSON")
            return json_data
            
        except (RetryableError, NonRetryableError, ResourceExhaustionError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error reading JSON from GCS: {str(e)}")
            raise MetricsPipelineError(f"Failed to read JSON from GCS: {str(e)}")
    
    def validate_json(self, json_data: List[Dict]) -> List[Dict]:
        """
        Validate JSON data for required fields and duplicates with enhanced validation
        
        Args:
            json_data: List of metric definitions
            
        Returns:
            List of validated metric definitions
            
        Raises:
            MetricsPipelineError: If validation fails
        """
        if not json_data:
            raise NonRetryableError("JSON data cannot be empty")
        
        required_fields = [
            'metric_id', 'metric_name', 'metric_type', 
            'sql', 'dependency', 'target_table'
        ]
        
        logger.info(f"Validating {len(json_data)} JSON records")
        monitor_resources()
        
        # Track metric IDs to check for duplicates
        metric_ids = set()
        target_tables = set()
        dependencies = set()
        
        validation_errors = []
        
        for i, record in enumerate(json_data):
            record_errors = []
            
            # Check if record is a dictionary
            if not isinstance(record, dict):
                record_errors.append(f"Record {i}: Expected dictionary, got {type(record)}")
                continue
            
            # Check for required fields
            for field in required_fields:
                if field not in record:
                    record_errors.append(f"Record {i}: Missing required field '{field}'")
                    continue
                
                value = record[field]
                # Enhanced validation for empty/whitespace-only strings
                if value is None:
                    record_errors.append(f"Record {i}: Field '{field}' cannot be null")
                elif isinstance(value, str) and value.strip() == "":
                    record_errors.append(f"Record {i}: Field '{field}' cannot be empty or contain only whitespace")
                elif not isinstance(value, str):
                    record_errors.append(f"Record {i}: Field '{field}' must be a string, got {type(value)}")
            
            # Skip further validation if basic field validation failed
            if record_errors:
                validation_errors.extend(record_errors)
                continue
            
            # Check for duplicate metric IDs
            metric_id = record['metric_id'].strip()
            if metric_id in metric_ids:
                record_errors.append(f"Record {i}: Duplicate metric_id '{metric_id}' found")
            else:
                metric_ids.add(metric_id)
            
            # Validate metric_id format (alphanumeric, underscore, dash)
            if not re.match(r'^[a-zA-Z0-9_-]+$', metric_id):
                record_errors.append(f"Record {i}: Invalid metric_id format '{metric_id}'. Only alphanumeric, underscore, and dash allowed")
            
            # Validate metric_type
            valid_metric_types = ['ratio', 'count', 'percentage', 'average', 'sum']
            metric_type = record['metric_type'].strip().lower()
            if metric_type not in valid_metric_types:
                record_errors.append(f"Record {i}: Invalid metric_type '{metric_type}'. Must be one of: {valid_metric_types}")
            
            # Validate target_table format (should look like project.dataset.table)
            target_table = record['target_table'].strip()
            if not target_table:
                record_errors.append(f"Record {i}: target_table cannot be empty")
            else:
                # Basic validation for BigQuery table format
                table_parts = target_table.split('.')
                if len(table_parts) != 3:
                    record_errors.append(f"Record {i}: target_table '{target_table}' must be in format 'project.dataset.table'")
                else:
                    # Check each part is not empty and follows naming conventions
                    part_names = ['project', 'dataset', 'table']
                    for part_idx, part in enumerate(table_parts):
                        if not part.strip():
                            record_errors.append(f"Record {i}: target_table '{target_table}' has empty {part_names[part_idx]} part")
                        elif not re.match(r'^[a-zA-Z0-9_-]+$', part):
                            record_errors.append(f"Record {i}: target_table '{target_table}' has invalid {part_names[part_idx]} '{part}'. Only alphanumeric, underscore, and dash allowed")
                
                target_tables.add(target_table)
            
            # Validate dependency format
            dependency = record['dependency'].strip()
            if not re.match(r'^[a-zA-Z0-9_-]+$', dependency):
                record_errors.append(f"Record {i}: Invalid dependency format '{dependency}'. Only alphanumeric, underscore, and dash allowed")
            dependencies.add(dependency)
            
            # Validate SQL query
            sql_query = record['sql'].strip()
            if not sql_query:
                record_errors.append(f"Record {i}: SQL query cannot be empty")
            else:
                # Basic SQL injection prevention
                dangerous_patterns = [
                    r';\s*DROP\s+', r';\s*DELETE\s+', r';\s*UPDATE\s+', r';\s*INSERT\s+', 
                    r';\s*ALTER\s+', r';\s*CREATE\s+', r';\s*TRUNCATE\s+', r'--', r'/\*'
                ]
                for pattern in dangerous_patterns:
                    if re.search(pattern, sql_query, re.IGNORECASE):
                        record_errors.append(f"Record {i}: SQL contains potentially dangerous pattern: {pattern}")
                
                # Check for valid placeholders
                currently_count = len(re.findall(r'\{currently\}', sql_query))
                partition_info_count = len(re.findall(r'\{partition_info\}', sql_query))
                
                if currently_count == 0 and partition_info_count == 0:
                    logger.warning(f"Record {i}: SQL query contains no placeholders ({{currently}} or {{partition_info}})")
                else:
                    logger.debug(f"Record {i}: Found {currently_count} {{currently}} and {partition_info_count} {{partition_info}} placeholders")
                
                # Validate SQL query structure
                if not re.search(r'\bSELECT\b', sql_query, re.IGNORECASE):
                    record_errors.append(f"Record {i}: SQL query must contain a SELECT statement")
            
            validation_errors.extend(record_errors)
        
        # Report all validation errors at once
        if validation_errors:
            error_summary = f"JSON validation failed with {len(validation_errors)} errors:\n" + "\n".join(validation_errors)
            raise NonRetryableError(error_summary)
        
        logger.info(f"Successfully validated {len(json_data)} records")
        logger.info(f"Found {len(metric_ids)} unique metric IDs")
        logger.info(f"Found {len(target_tables)} target tables: {sorted(target_tables)}")
        logger.info(f"Found {len(dependencies)} dependencies: {sorted(dependencies)}")
        
        return json_data
    
    def find_placeholder_positions(self, sql: str) -> List[Tuple[str, int, int]]:
        """
        Find all {currently} and {partition_info} placeholders in SQL with their positions
        
        Args:
            sql: SQL query string
            
        Returns:
            List of tuples (placeholder_type, start_pos, end_pos)
        """
        if not sql:
            return []
        
        placeholders = []
        
        try:
            # Find {currently} placeholders
            currently_pattern = r'\{currently\}'
            for match in re.finditer(currently_pattern, sql):
                placeholders.append(('currently', match.start(), match.end()))
            
            # Find {partition_info} placeholders
            partition_info_pattern = r'\{partition_info\}'
            for match in re.finditer(partition_info_pattern, sql):
                placeholders.append(('partition_info', match.start(), match.end()))
            
            # Sort by position for consistent replacement
            placeholders.sort(key=lambda x: x[1])
            
        except Exception as e:
            logger.error(f"Error finding placeholders in SQL: {str(e)}")
            raise MetricsPipelineError(f"Failed to parse SQL placeholders: {str(e)}")
        
        return placeholders
    
    def get_table_for_placeholder(self, sql: str, placeholder_pos: int) -> Optional[Tuple[str, str]]:
        """
        Find the table associated with a placeholder based on its position in the SQL
        
        Args:
            sql: SQL query string
            placeholder_pos: Position of the placeholder in the SQL
            
        Returns:
            Tuple (dataset, table_name) or None if not found
        """
        if not sql:
            return None
        
        try:
            # Find all table references with their positions
            table_pattern = r'`([^.]+)\.([^.]+)\.([^`]+)`'
            
            # Find the table reference that comes before this placeholder
            best_table = None
            best_distance = float('inf')
            
            for match in re.finditer(table_pattern, sql):
                table_end_pos = match.end()
                
                # Check if this table comes before the placeholder
                if table_end_pos < placeholder_pos:
                    distance = placeholder_pos - table_end_pos
                    if distance < best_distance:
                        best_distance = distance
                        project, dataset, table = match.groups()
                        best_table = (dataset, table)
            
            return best_table
            
        except Exception as e:
            logger.error(f"Error finding table for placeholder: {str(e)}")
            return None
    
    @retry_on_failure(max_retries=2, delay=1.0)
    def get_partition_dt(self, project_dataset: str, table_name: str, partition_info_table: str) -> Optional[str]:
        """
        Get latest partition_dt from metadata table with enhanced error handling
        
        Args:
            project_dataset: Dataset name
            table_name: Table name
            partition_info_table: Metadata table name
            
        Returns:
            Latest partition date as string or None
        """
        if not all([project_dataset, table_name, partition_info_table]):
            logger.error("Invalid parameters for get_partition_dt")
            return None
        
        try:
            # Validate table name format
            if not re.match(r'^[a-zA-Z0-9_.-]+$', partition_info_table):
                raise NonRetryableError(f"Invalid partition_info_table format: {partition_info_table}")
            
            # Use parameterized query to prevent SQL injection
            query = f"""
            SELECT partition_dt 
            FROM `{partition_info_table}` 
            WHERE project_dataset = @project_dataset 
            AND table_name = @table_name
            ORDER BY partition_dt DESC
            LIMIT 1
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("project_dataset", "STRING", project_dataset),
                    bigquery.ScalarQueryParameter("table_name", "STRING", table_name)
                ]
            )
            
            logger.info(f"Querying partition info for {project_dataset}.{table_name}")
            
            query_job = self.bq_client.query(query, job_config=job_config)
            results = query_job.result(timeout=60)  # Add timeout
            
            for row in results:
                partition_dt = row.partition_dt
                if isinstance(partition_dt, datetime):
                    return partition_dt.strftime('%Y-%m-%d')
                elif isinstance(partition_dt, str):
                    # Validate date format
                    try:
                        datetime.strptime(partition_dt, '%Y-%m-%d')
                        return partition_dt
                    except ValueError:
                        logger.warning(f"Invalid date format in partition_dt: {partition_dt}")
                        return None
                else:
                    logger.warning(f"Unexpected partition_dt type: {type(partition_dt)}")
                    return None
            
            logger.warning(f"No partition info found for {project_dataset}.{table_name}")
            return None
            
        except NotFound:
            logger.error(f"Partition info table not found: {partition_info_table}")
            return None
        except Forbidden:
            logger.error(f"Access denied to partition info table: {partition_info_table}")
            return None
        except DeadlineExceeded:
            raise RetryableError(f"Query timeout getting partition_dt for {project_dataset}.{table_name}")
        except Exception as e:
            logger.error(f"Failed to get partition_dt for {project_dataset}.{table_name}: {str(e)}")
            raise RetryableError(f"Failed to get partition_dt: {str(e)}")
    
    def replace_sql_placeholders(self, sql: str, run_date: str, partition_info_table: str) -> str:
        """
        Replace {currently} and {partition_info} placeholders in SQL with appropriate dates
        
        Args:
            sql: SQL query string with placeholders
            run_date: CLI provided run date
            partition_info_table: Metadata table name
            
        Returns:
            SQL with all placeholders replaced
        """
        if not sql:
            raise NonRetryableError("SQL query cannot be empty")
        
        if not run_date:
            raise NonRetryableError("run_date cannot be empty")
        
        # Validate run_date format
        try:
            datetime.strptime(run_date, '%Y-%m-%d')
        except ValueError:
            raise NonRetryableError(f"Invalid run_date format: {run_date}. Expected YYYY-MM-DD")
        
        try:
            # Find all placeholders
            placeholders = self.find_placeholder_positions(sql)
            
            if not placeholders:
                logger.info("No placeholders found in SQL")
                return sql
            
            logger.info(f"Found {len(placeholders)} placeholders: {[p[0] for p in placeholders]}")
            
            # Process replacements from end to beginning to preserve positions
            final_sql = sql
            
            for placeholder_type, start_pos, end_pos in reversed(placeholders):
                if shutdown_requested:
                    raise MetricsPipelineError("Pipeline shutdown requested")
                
                if placeholder_type == 'currently':
                    replacement_date = run_date
                    logger.info(f"Replacing {{currently}} with run_date: {replacement_date}")
                    
                elif placeholder_type == 'partition_info':
                    # Find the table associated with this placeholder
                    table_info = self.get_table_for_placeholder(sql, start_pos)
                    
                    if table_info:
                        dataset, table_name = table_info
                        replacement_date = self.get_partition_dt(dataset, table_name, partition_info_table)
                        
                        if not replacement_date:
                            raise MetricsPipelineError(
                                f"Could not determine partition_dt for table {dataset}.{table_name}. "
                                f"Check if partition info exists in {partition_info_table}"
                            )
                        
                        logger.info(f"Replacing {{partition_info}} with partition_dt: {replacement_date} for table {dataset}.{table_name}")
                    else:
                        raise MetricsPipelineError(
                            f"Could not find table reference for {{partition_info}} placeholder at position {start_pos}. "
                            f"Ensure table is referenced in backticks before the placeholder."
                        )
                
                # Replace the placeholder with the date (with SQL injection prevention)
                escaped_date = replacement_date.replace("'", "''")  # Escape single quotes
                final_sql = final_sql[:start_pos] + f"'{escaped_date}'" + final_sql[end_pos:]
            
            logger.info(f"Replaced {len(placeholders)} placeholders in SQL")
            logger.debug(f"Final SQL: {final_sql}")
            
            return final_sql
            
        except (RetryableError, NonRetryableError, MetricsPipelineError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error replacing SQL placeholders: {str(e)}")
            raise MetricsPipelineError(f"Failed to replace SQL placeholders: {str(e)}")
    
    def normalize_numeric_value(self, value: Union[int, float, Decimal, None]) -> Optional[str]:
        """
        Normalize numeric values to string representation to preserve precision
        
        Args:
            value: Numeric value of any type
            
        Returns:
            String representation of the number or None
        """
        if value is None:
            return None
        
        try:
            # Handle different numeric types with precision preservation
            if isinstance(value, Decimal):
                # Keep as string to preserve precision
                return str(value)
            elif isinstance(value, (int, float)):
                # Convert to Decimal first to handle large numbers properly
                decimal_val = Decimal(str(value))
                return str(decimal_val)
            elif isinstance(value, str):
                # Try to parse as Decimal to validate it's a valid number
                try:
                    decimal_val = Decimal(value)
                    return str(decimal_val)
                except:
                    logger.warning(f"Could not parse string as number: {value}")
                    return None
            else:
                # Try to convert to string and then to Decimal
                decimal_val = Decimal(str(value))
                return str(decimal_val)
                
        except (ValueError, TypeError, OverflowError, Exception) as e:
            logger.warning(f"Could not normalize numeric value: {value}, error: {e}")
            return None
    
    def safe_decimal_conversion(self, value: Optional[str]) -> Optional[Decimal]:
        """
        Safely convert string to Decimal for BigQuery
        
        Args:
            value: String representation of number
            
        Returns:
            Decimal value or None
        """
        if value is None:
            return None
        
        try:
            return Decimal(value)
        except (ValueError, TypeError, OverflowError):
            logger.warning(f"Could not convert to Decimal: {value}")
            return None
    
    def check_dependencies_exist(self, json_data: List[Dict], dependencies: List[str]) -> None:
        """
        Check if all specified dependencies exist in the JSON data
        
        Args:
            json_data: List of metric definitions
            dependencies: List of dependencies to check
            
        Raises:
            MetricsPipelineError: If any dependency is missing
        """
        available_dependencies = set(record['dependency'] for record in json_data)
        missing_dependencies = set(dependencies) - available_dependencies
        
        if missing_dependencies:
            raise MetricsPipelineError(
                f"Missing dependencies in JSON data: {missing_dependencies}. "
                f"Available dependencies: {available_dependencies}"
            )
        
        logger.info(f"All dependencies found: {dependencies}")
    
    @retry_on_failure(max_retries=3, delay=1.0)
    def execute_sql(self, sql: str, run_date: str, partition_info_table: str, metric_id: Optional[str] = None) -> Dict:
        """
        Execute SQL query with dynamic placeholder replacement and enhanced error handling
        
        Args:
            sql: SQL query string with {currently} and {partition_info} placeholders
            run_date: CLI provided run date
            partition_info_table: Metadata table name
            metric_id: Optional metric ID for better error reporting
            
        Returns:
            Dictionary with query results
        """
        if not sql:
            raise NonRetryableError("SQL query cannot be empty")
        
        metric_prefix = f"Metric '{metric_id}': " if metric_id else ""
        
        try:
            monitor_resources()  # Check resources before expensive operation
            
            # Replace placeholders with appropriate dates
            final_sql = self.replace_sql_placeholders(sql, run_date, partition_info_table)
            
            logger.info(f"{metric_prefix}Executing SQL with placeholder replacements")
            
            # Configure query job with timeout and other safety measures
            job_config = bigquery.QueryJobConfig(
                query_parameters=[],
                use_query_cache=True,
                maximum_bytes_billed=1000000000,  # 1GB limit
                job_timeout_ms=300000,  # 5 minute timeout
                dry_run=False
            )
            
            # Execute query with retry logic
            query_job = self.bq_client.query(final_sql, job_config=job_config)
            
            try:
                results = query_job.result(timeout=300)  # 5 minute timeout
            except DeadlineExceeded:
                raise RetryableError(f"{metric_prefix}Query timeout exceeded. Query may be too complex or data too large.")
            except Exception as e:
                if "Query exceeded limit" in str(e):
                    raise NonRetryableError(f"{metric_prefix}Query exceeded resource limits: {str(e)}")
                elif "Syntax error" in str(e) or "Invalid" in str(e):
                    raise NonRetryableError(f"{metric_prefix}SQL syntax error: {str(e)}")
                else:
                    raise RetryableError(f"{metric_prefix}Query execution failed: {str(e)}")
            
            # Validate query returned results
            row_count = 0
            result_dict = {
                'metric_output': None,
                'numerator_value': None,
                'denominator_value': None,
                'business_data_date': None
            }
            
            for row in results:
                row_count += 1
                
                if row_count > 1:
                    logger.warning(f"{metric_prefix}Query returned {row_count} rows, using first row only")
                
                # Convert row to dictionary
                try:
                    row_dict = dict(row)
                except Exception as e:
                    raise MetricsPipelineError(f"{metric_prefix}Failed to convert query result to dictionary: {str(e)}")
                
                # Map columns to result dictionary with precision preservation
                for key in result_dict.keys():
                    if key in row_dict:
                        value = row_dict[key]
                        # Normalize numeric values to preserve precision
                        if key in ['metric_output', 'numerator_value', 'denominator_value']:
                            result_dict[key] = self.normalize_numeric_value(value)
                        else:
                            result_dict[key] = value
                
                break  # Take first row only
            
            if row_count == 0:
                raise MetricsPipelineError(f"{metric_prefix}Query returned no results")
            
            # Validate required fields are present
            if result_dict['metric_output'] is None:
                logger.warning(f"{metric_prefix}Query returned NULL metric_output")
            
            # Validate denominator_value is not zero
            if result_dict['denominator_value'] is not None:
                try:
                    denominator_decimal = self.safe_decimal_conversion(result_dict['denominator_value'])
                    if denominator_decimal is not None:
                        if denominator_decimal == 0:
                            error_msg = f"Invalid denominator value: denominator_value is 0. Cannot calculate metrics with zero denominator."
                            logger.error(f"{metric_prefix}{error_msg}")
                            raise NonRetryableError(f"{metric_prefix}{error_msg}")
                        elif denominator_decimal < 0:
                            error_msg = f"Invalid denominator value: denominator_value is negative ({denominator_decimal}). Negative denominators are not allowed."
                            logger.error(f"{metric_prefix}{error_msg}")
                            raise NonRetryableError(f"{metric_prefix}{error_msg}")
                        # Log warning for very small positive denominators that might cause precision issues
                        elif abs(denominator_decimal) < Decimal('0.0000001'):
                            warning_msg = f"Very small denominator value detected: {denominator_decimal}. This may cause precision issues."
                            logger.warning(f"{metric_prefix}{warning_msg}")
                except (ValueError, TypeError):
                    # If we can't convert to decimal, log warning but continue
                    logger.warning(f"{metric_prefix}Could not validate denominator_value: {result_dict['denominator_value']}")
            
            # Calculate business_data_date (one day before run_date)
            try:
                ref_date = datetime.strptime(run_date, '%Y-%m-%d')
                business_date = ref_date - timedelta(days=1)
                result_dict['business_data_date'] = business_date.strftime('%Y-%m-%d')
            except ValueError as e:
                raise NonRetryableError(f"{metric_prefix}Invalid run_date format: {run_date}")
            
            logger.info(f"{metric_prefix}Successfully executed SQL query")
            return result_dict
            
        except (RetryableError, NonRetryableError, MetricsPipelineError):
            raise
        except Exception as e:
            error_msg = f"Unexpected error executing SQL: {str(e)}"
            logger.error(f"{metric_prefix}{error_msg}")
            raise MetricsPipelineError(f"{metric_prefix}{error_msg}")
    
    def rollback_metric(self, metric_id: str, target_table: str, partition_dt: str) -> None:
        """
        Rollback a specific metric from the target table
        
        Args:
            metric_id: Metric ID to rollback
            target_table: Target BigQuery table
            partition_dt: Partition date for the metric
        """
        try:
            delete_query = f"""
            DELETE FROM `{target_table}` 
            WHERE metric_id = '{metric_id}' 
            AND partition_dt = '{partition_dt}'
            AND pipeline_execution_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
            """
            
            logger.info(f"Rolling back metric {metric_id} from {target_table}")
            
            query_job = self.bq_client.query(delete_query)
            query_job.result()
            
            logger.info(f"Successfully rolled back metric {metric_id}")
            
        except Exception as e:
            logger.error(f"Failed to rollback metric {metric_id}: {str(e)}")
    
    def rollback_processed_metrics(self, target_table: str, partition_dt: str) -> None:
        """
        Rollback all processed metrics in case of failure
        Note: This only rolls back newly inserted metrics, not overwritten ones
        
        Args:
            target_table: Target BigQuery table
            partition_dt: Partition date for rollback
        """
        logger.info("Starting rollback of processed metrics")
        
        if not self.processed_metrics:
            logger.info("No metrics to rollback")
            return
        
        # Only rollback newly inserted metrics (not overwritten ones)
        new_metrics = [mid for mid in self.processed_metrics if mid not in self.overwritten_metrics]
        
        if new_metrics:
            logger.info(f"Rolling back {len(new_metrics)} newly inserted metrics")
            for metric_id in new_metrics:
                try:
                    self.rollback_metric(metric_id, target_table, partition_dt)
                except Exception as e:
                    logger.error(f"Failed to rollback metric {metric_id}: {str(e)}")
        else:
            logger.info("No newly inserted metrics to rollback")
        
        if self.overwritten_metrics:
            logger.warning(f"Note: {len(self.overwritten_metrics)} overwritten metrics cannot be automatically restored: {self.overwritten_metrics}")
        
        logger.info("Rollback process completed")
    
    def process_metrics(self, json_data: List[Dict], run_date: str, 
                       dependencies: List[str], partition_info_table: str) -> Dict[str, DataFrame]:
        """
        Process metrics and create Spark DataFrames grouped by target_table
        
        Args:
            json_data: List of metric definitions
            run_date: CLI provided run date
            dependencies: List of dependencies to process
            partition_info_table: Metadata table name
            
        Returns:
            Dictionary mapping target_table to Spark DataFrame with processed metrics
        """
        logger.info(f"Processing metrics for dependencies: {dependencies}")
        
        # Check if all dependencies exist
        self.check_dependencies_exist(json_data, dependencies)

        partition_dt = datetime.now().strftime('%Y-%m-%d')
        logger.info(f"Using pipeline run date as partition_dt: {partition_dt}")
        
        # Filter records by dependency
        filtered_data = [
            record for record in json_data 
            if record['dependency'] in dependencies
        ]
        
        if not filtered_data:
            raise MetricsPipelineError(
                f"No records found for dependencies: {dependencies}"
            )
        
        logger.info(f"Found {len(filtered_data)} records to process")
        
        # Group records by target_table
        records_by_table = {}
        for record in filtered_data:
            target_table = record['target_table'].strip()
            if target_table not in records_by_table:
                records_by_table[target_table] = []
            records_by_table[target_table].append(record)
        
        logger.info(f"Records grouped into {len(records_by_table)} target tables: {list(records_by_table.keys())}")
        
        # Process each group and create DataFrames
        result_dfs = {}
        
        for target_table, records in records_by_table.items():
            logger.info(f"Processing {len(records)} metrics for target table: {target_table}")
            
            # Process each record for this target table
            processed_records = []
            
            for record in records:
                try:
                    # Execute SQL and get results
                    sql_results = self.execute_sql(
                        record['sql'], 
                        run_date, 
                        partition_info_table,
                        record['metric_id'] # Pass metric_id for error reporting
                    )
                    
                    # Build final record with precision preservation
                    final_record = {
                        'metric_id': record['metric_id'],
                        'metric_name': record['metric_name'],
                        'metric_type': record['metric_type'],
                        'numerator_value': self.safe_decimal_conversion(sql_results['numerator_value']),
                        'denominator_value': self.safe_decimal_conversion(sql_results['denominator_value']),
                        'metric_output': self.safe_decimal_conversion(sql_results['metric_output']),
                        'business_data_date': sql_results['business_data_date'],
                        'partition_dt': partition_dt,
                        'pipeline_execution_ts': datetime.utcnow()
                    }
                    
                    processed_records.append(final_record)
                    logger.info(f"Successfully processed metric_id: {record['metric_id']} for table: {target_table}")
                    
                except Exception as e:
                    logger.error(f"Failed to process metric_id {record['metric_id']} for table {target_table}: {str(e)}")
                    raise MetricsPipelineError(
                        f"Failed to process metric_id {record['metric_id']} for table {target_table}: {str(e)}"
                    )
            
            # Create Spark DataFrame for this target table
            if processed_records:
                # Define explicit schema with high precision for numeric fields
                schema = StructType([
                    StructField("metric_id", StringType(), False),
                    StructField("metric_name", StringType(), False),
                    StructField("metric_type", StringType(), False),
                    StructField("numerator_value", DecimalType(38, 9), True),
                    StructField("denominator_value", DecimalType(38, 9), True),
                    StructField("metric_output", DecimalType(38, 9), True),
                    StructField("business_data_date", StringType(), False),
                    StructField("partition_dt", StringType(), False),
                    StructField("pipeline_execution_ts", TimestampType(), False)
                ])
                
                # Create DataFrame with explicit schema
                df = self.spark.createDataFrame(processed_records, schema)
                result_dfs[target_table] = df
                logger.info(f"Created DataFrame for {target_table} with {df.count()} records")
            else:
                logger.warning(f"No records processed for target table: {target_table}")
        
        if not result_dfs:
            raise MetricsPipelineError("No records were successfully processed for any target table")
        
        return result_dfs
    
    def get_bq_table_schema(self, table_name: str) -> List[bigquery.SchemaField]:
        """
        Get BigQuery table schema
        
        Args:
            table_name: Full table name (project.dataset.table)
            
        Returns:
            List of schema fields
        """
        try:
            logger.info(f"Getting schema for table: {table_name}")
            table = self.bq_client.get_table(table_name)
            return table.schema
            
        except NotFound:
            raise MetricsPipelineError(f"Table not found: {table_name}")
        except Exception as e:
            logger.error(f"Failed to get table schema: {str(e)}")
            raise MetricsPipelineError(f"Failed to get table schema: {str(e)}")
    
    def align_schema_with_bq(self, df: DataFrame, target_table: str) -> DataFrame:
        """
        Align Spark DataFrame with BigQuery table schema
        
        Args:
            df: Spark DataFrame
            target_table: BigQuery table name
            
        Returns:
            Schema-aligned DataFrame
        """
        logger.info(f"Aligning DataFrame schema with BigQuery table: {target_table}")
        
        # Get BigQuery schema
        bq_schema = self.get_bq_table_schema(target_table)
        
        # Get current DataFrame columns
        current_columns = df.columns
        
        # Build list of columns in BigQuery schema order
        bq_columns = [field.name for field in bq_schema]
        
        # Drop extra columns not in BigQuery schema
        columns_to_keep = [col for col in current_columns if col in bq_columns]
        columns_to_drop = [col for col in current_columns if col not in bq_columns]
        
        if columns_to_drop:
            logger.info(f"Dropping extra columns: {columns_to_drop}")
            df = df.drop(*columns_to_drop)
        
        # Reorder columns to match BigQuery schema
        df = df.select(*[col(c) for c in bq_columns if c in columns_to_keep])
        
        # Handle type conversions for BigQuery compatibility
        for field in bq_schema:
            if field.name in df.columns:
                if field.field_type == 'DATE':
                    df = df.withColumn(field.name, to_date(col(field.name)))
                elif field.field_type == 'TIMESTAMP':
                    df = df.withColumn(field.name, col(field.name).cast(TimestampType()))
                elif field.field_type == 'NUMERIC':
                    df = df.withColumn(field.name, col(field.name).cast(DecimalType(38, 9)))
                elif field.field_type == 'FLOAT':
                    df = df.withColumn(field.name, col(field.name).cast(DoubleType()))
        
        logger.info(f"Schema alignment complete. Final columns: {df.columns}")
        return df
    
    @retry_on_failure(max_retries=3, delay=1.0)
    def write_to_bq(self, df: DataFrame, target_table: str) -> None:
        """
        Write DataFrame to BigQuery table with transaction safety
        
        Args:
            df: Spark DataFrame to write
            target_table: Target BigQuery table
        """
        try:
            logger.info(f"Writing DataFrame to BigQuery table: {target_table}")
            
            # Collect metric IDs for rollback tracking
            metric_ids = [row['metric_id'] for row in df.select('metric_id').collect()]
            self.processed_metrics.extend(metric_ids)
            
            # Write to BigQuery using Spark BigQuery connector
            df.write \
                .format("bigquery") \
                .option("table", target_table) \
                .option("writeMethod", "direct") \
                .mode("append") \
                .save()
            
            logger.info(f"Successfully wrote {df.count()} records to {target_table}")
            
        except Exception as e:
            logger.error(f"Failed to write to BigQuery: {str(e)}")
            raise MetricsPipelineError(f"Failed to write to BigQuery: {str(e)}")
    
    def check_existing_metrics(self, metric_ids: List[str], partition_dt: str, target_table: str) -> List[str]:
        """
        Check which metric IDs already exist in BigQuery table for the given partition date
        
        Args:
            metric_ids: List of metric IDs to check
            partition_dt: Partition date to check
            target_table: Target BigQuery table
            
        Returns:
            List of existing metric IDs
        """
        try:
            if not metric_ids:
                return []
            
            # Escape single quotes in metric IDs for safety
            escaped_metric_ids = [mid.replace("'", "''") for mid in metric_ids]
            metric_ids_str = "', '".join(escaped_metric_ids)
            
            query = f"""
            SELECT DISTINCT metric_id 
            FROM `{target_table}` 
            WHERE metric_id IN ('{metric_ids_str}') 
            AND partition_dt = '{partition_dt}'
            """
            
            logger.info(f"Checking existing metrics for partition_dt: {partition_dt}")
            logger.debug(f"Query: {query}")
            
            query_job = self.bq_client.query(query)
            results = query_job.result()
            
            existing_metrics = [row.metric_id for row in results]
            
            if existing_metrics:
                logger.info(f"Found {len(existing_metrics)} existing metrics: {existing_metrics}")
            else:
                logger.info("No existing metrics found")
            
            return existing_metrics
            
        except Exception as e:
            logger.error(f"Failed to check existing metrics: {str(e)}")
            raise MetricsPipelineError(f"Failed to check existing metrics: {str(e)}")
    
    @retry_on_failure(max_retries=3, delay=1.0)
    def delete_existing_metrics(self, metric_ids: List[str], partition_dt: str, target_table: str) -> None:
        """
        Delete existing metrics from BigQuery table for the given partition date
        
        Args:
            metric_ids: List of metric IDs to delete
            partition_dt: Partition date for deletion
            target_table: Target BigQuery table
        """
        try:
            if not metric_ids:
                logger.info("No metrics to delete")
                return
            
            # Escape single quotes in metric IDs for safety
            escaped_metric_ids = [mid.replace("'", "''") for mid in metric_ids]
            metric_ids_str = "', '".join(escaped_metric_ids)
            
            delete_query = f"""
            DELETE FROM `{target_table}` 
            WHERE metric_id IN ('{metric_ids_str}') 
            AND partition_dt = '{partition_dt}'
            """
            
            logger.info(f"Deleting existing metrics: {metric_ids} for partition_dt: {partition_dt}")
            logger.debug(f"Delete query: {delete_query}")
            
            query_job = self.bq_client.query(delete_query)
            results = query_job.result()
            
            # Get the number of deleted rows
            deleted_count = results.num_dml_affected_rows if hasattr(results, 'num_dml_affected_rows') else 0
            
            logger.info(f"Successfully deleted {deleted_count} existing records for metrics: {metric_ids}")
            
        except Exception as e:
            logger.error(f"Failed to delete existing metrics: {str(e)}")
            raise MetricsPipelineError(f"Failed to delete existing metrics: {str(e)}")
    
    @retry_on_failure(max_retries=3, delay=1.0)
    def write_to_bq_with_overwrite(self, df: DataFrame, target_table: str) -> None:
        """
        Write DataFrame to BigQuery table with overwrite capability for existing metrics
        
        Args:
            df: Spark DataFrame to write
            target_table: Target BigQuery table
        """
        try:
            logger.info(f"Writing DataFrame to BigQuery table with overwrite: {target_table}")
            
            # Track target table for rollback
            self.target_tables.add(target_table)
            
            # Collect metric IDs and partition date from the DataFrame
            metric_records = df.select('metric_id', 'partition_dt').distinct().collect()
            
            if not metric_records:
                logger.warning("No records to process")
                return
            
            # Get partition date (assuming all records have the same partition_dt)
            partition_dt = metric_records[0]['partition_dt']
            metric_ids = [row['metric_id'] for row in metric_records]
            
            logger.info(f"Processing {len(metric_ids)} metrics for partition_dt: {partition_dt}")
            
            # Check which metrics already exist
            existing_metrics = self.check_existing_metrics(metric_ids, partition_dt, target_table)
            
            # Track overwritten vs new metrics
            new_metrics = [mid for mid in metric_ids if mid not in existing_metrics]
            
            # Delete existing metrics if any
            if existing_metrics:
                logger.info(f"Overwriting {len(existing_metrics)} existing metrics: {existing_metrics}")
                self.delete_existing_metrics(existing_metrics, partition_dt, target_table)
                # Track overwritten metrics separately
                self.overwritten_metrics.extend(existing_metrics)
            
            if new_metrics:
                logger.info(f"Adding {len(new_metrics)} new metrics: {new_metrics}")
            
            # Add all metric IDs to processed metrics for rollback tracking
            self.processed_metrics.extend(metric_ids)
            
            # Write the DataFrame to BigQuery
            df.write \
                .format("bigquery") \
                .option("table", target_table) \
                .option("writeMethod", "direct") \
                .mode("append") \
                .save()
            
            logger.info(f"Successfully wrote {df.count()} records to {target_table}")
            logger.info(f"Summary: {len(existing_metrics)} overwritten, {len(new_metrics)} new metrics")
            
        except Exception as e:
            logger.error(f"Failed to write to BigQuery with overwrite: {str(e)}")
            raise MetricsPipelineError(f"Failed to write to BigQuery with overwrite: {str(e)}")
    
    def rollback_all_processed_metrics(self, partition_dt: str) -> None:
        """
        Rollback all processed metrics from all target tables in case of failure
        
        Args:
            partition_dt: Partition date for rollback
        """
        logger.info("Starting rollback of processed metrics from all target tables")
        
        if not self.processed_metrics:
            logger.info("No metrics to rollback")
            return
        
        if not self.target_tables:
            logger.info("No target tables to rollback from")
            return
        
        # Only rollback newly inserted metrics (not overwritten ones)
        new_metrics = [mid for mid in self.processed_metrics if mid not in self.overwritten_metrics]
        
        if new_metrics:
            logger.info(f"Rolling back {len(new_metrics)} newly inserted metrics from {len(self.target_tables)} tables")
            
            for target_table in self.target_tables:
                logger.info(f"Rolling back metrics from table: {target_table}")
                
                # Find metrics that were inserted into this specific table
                # We need to filter metrics by target table if we have that information
                for metric_id in new_metrics:
                    try:
                        self.rollback_metric(metric_id, target_table, partition_dt)
                    except Exception as e:
                        logger.error(f"Failed to rollback metric {metric_id} from table {target_table}: {str(e)}")
        else:
            logger.info("No newly inserted metrics to rollback")
        
        if self.overwritten_metrics:
            logger.warning(f"Note: {len(self.overwritten_metrics)} overwritten metrics cannot be automatically restored: {self.overwritten_metrics}")
        
        logger.info("Rollback process completed")


@contextmanager
def managed_spark_session(app_name: str = "MetricsPipeline"):
    """
    Context manager for Spark session with proper cleanup
    
    Args:
        app_name: Spark application name
        
    Yields:
        SparkSession instance
    """
    spark = None
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        logger.info(f"Spark session created successfully: {app_name}")
        yield spark
        
    except Exception as e:
        logger.error(f"Error in Spark session: {str(e)}")
        raise
    finally:
        if spark:
            try:
                spark.stop()
                logger.info("Spark session stopped successfully")
            except Exception as e:
                logger.error(f"Error stopping Spark session: {str(e)}")


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='PySpark BigQuery Metrics Pipeline'
    )
    
    parser.add_argument(
        '--gcs_path', 
        required=True, 
        help='GCS path to JSON input file'
    )
    parser.add_argument(
        '--run_date', 
        required=True, 
        help='Run date in YYYY-MM-DD format'
    )
    parser.add_argument(
        '--dependencies', 
        required=True, 
        help='Comma-separated list of dependencies to process'
    )
    parser.add_argument(
        '--partition_info_table', 
        required=True, 
        help='BigQuery table for partition info (project.dataset.table)'
    )
    
    return parser.parse_args()


def validate_date_format(date_str: str) -> None:
    """Validate date format"""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise MetricsPipelineError(f"Invalid date format: {date_str}. Expected YYYY-MM-DD")


def main():
    """Main function with comprehensive error handling and resource management"""
    pipeline = None
    partition_dt = None
    spark = None
    start_time = datetime.utcnow()
    
    try:
        # Validate environment before starting
        validate_environment()
        
        # Parse arguments with validation
        args = parse_arguments()
        
        # Validate date format
        validate_date_format(args.run_date)
        
        # Parse dependencies (strip whitespace)
        dependencies = [dep.strip() for dep in args.dependencies.split(',') if dep.strip()]
        
        if not dependencies:
            raise NonRetryableError("No valid dependencies provided")
        
        # Validate dependencies format
        for dep in dependencies:
            if not re.match(r'^[a-zA-Z0-9_-]+$', dep):
                raise NonRetryableError(f"Invalid dependency format: {dep}")
        
        logger.info("=" * 80)
        logger.info("STARTING METRICS PIPELINE")
        logger.info("=" * 80)
        logger.info(f"Execution ID: {str(uuid.uuid4())}")
        logger.info(f"Start Time: {start_time.isoformat()}")
        logger.info(f"GCS Path: {args.gcs_path}")
        logger.info(f"Run Date: {args.run_date}")
        logger.info(f"Dependencies: {dependencies}")
        logger.info(f"Partition Info Table: {args.partition_info_table}")
        logger.info("Pipeline will check for existing metrics and overwrite them if found")
        logger.info("Target tables will be read from JSON configuration")
        logger.info("JSON must contain: metric_id, metric_name, metric_type, sql, dependency, target_table")
        logger.info("SQL placeholders: {currently} = run_date, {partition_info} = partition_dt from metadata table")
        
        # Initial resource check
        monitor_resources()
        
        # Use managed Spark session with enhanced error handling
        with managed_spark_session("MetricsPipeline") as spark:
            # Initialize BigQuery client with retry configuration
            try:
                bq_client = bigquery.Client()
                logger.info("BigQuery client initialized successfully")
            except Exception as e:
                raise NonRetryableError(f"Failed to initialize BigQuery client: {str(e)}")
            
            # Initialize pipeline with context manager for cleanup
            with MetricsPipeline(spark, bq_client) as pipeline:
                # Store partition_dt for potential rollback
                partition_dt = datetime.now().strftime('%Y-%m-%d')
                
                # Step 1: Reading JSON from GCS
                logger.info("=" * 50)
                logger.info("STEP 1: Reading JSON from GCS")
                logger.info("=" * 50)
                
                if shutdown_requested:
                    raise MetricsPipelineError("Pipeline shutdown requested")
                
                json_data = pipeline.read_json_from_gcs(args.gcs_path)
                logger.info(f"✅ Successfully read {len(json_data)} records from GCS")
                
                # Step 2: Validating JSON data
                logger.info("=" * 50)
                logger.info("STEP 2: Validating JSON data")
                logger.info("=" * 50)
                
                if shutdown_requested:
                    raise MetricsPipelineError("Pipeline shutdown requested")
                
                validated_data = pipeline.validate_json(json_data)
                logger.info(f"✅ Successfully validated {len(validated_data)} records")
                
                # Step 3: Processing metrics
                logger.info("=" * 50)
                logger.info("STEP 3: Processing metrics")
                logger.info("=" * 50)
                
                if shutdown_requested:
                    raise MetricsPipelineError("Pipeline shutdown requested")
                
                metrics_dfs = pipeline.process_metrics(
                    validated_data, 
                    args.run_date, 
                    dependencies, 
                    args.partition_info_table
                )
                
                total_metrics = sum(df.count() for df in metrics_dfs.values())
                logger.info(f"✅ Successfully processed {total_metrics} metrics across {len(metrics_dfs)} target tables")
                
                # Step 4: Aligning schema with BigQuery and writing to tables
                logger.info("=" * 50)
                logger.info("STEP 4: Writing to BigQuery tables")
                logger.info("=" * 50)
                
                successful_tables = []
                failed_tables = []
                
                # Process each target table
                for target_table, df in metrics_dfs.items():
                    if shutdown_requested:
                        logger.warning("Pipeline shutdown requested during table processing")
                        break
                    
                    try:
                        logger.info(f"🔄 Processing target table: {target_table}")
                        
                        # Check resources before processing each table
                        monitor_resources()
                        
                        # Align schema with BigQuery
                        aligned_df = pipeline.align_schema_with_bq(df, target_table)
                        
                        # Show schema and data for debugging (limited output)
                        logger.info(f"Schema for {target_table}:")
                        aligned_df.printSchema()
                        
                        # Show only first few rows to avoid overwhelming logs
                        sample_size = min(5, aligned_df.count())
                        if sample_size > 0:
                            aligned_df.limit(sample_size).show(truncate=False)
                        else:
                            logger.warning(f"No data to display for {target_table}")
                        
                        # Write to BigQuery with overwrite capability
                        logger.info(f"🔄 Writing to BigQuery table: {target_table}")
                        pipeline.write_to_bq_with_overwrite(aligned_df, target_table)
                        
                        successful_tables.append(target_table)
                        logger.info(f"✅ Successfully processed table: {target_table}")
                        
                    except Exception as e:
                        failed_tables.append((target_table, str(e)))
                        logger.error(f"❌ Failed to process table {target_table}: {str(e)}")
                        
                        # For critical errors, stop processing
                        if isinstance(e, (ResourceExhaustionError, NonRetryableError)):
                            raise e
                        
                        # For other errors, continue with remaining tables but log the error
                        pipeline.errors_encountered += 1
                
                # Summary of results
                logger.info("=" * 80)
                logger.info("PIPELINE EXECUTION SUMMARY")
                logger.info("=" * 80)
                
                execution_time = datetime.utcnow() - start_time
                logger.info(f"Total execution time: {execution_time}")
                
                if successful_tables:
                    logger.info(f"✅ Successfully processed {len(successful_tables)} tables:")
                    for table in successful_tables:
                        logger.info(f"  - {table}")
                
                if failed_tables:
                    logger.error(f"❌ Failed to process {len(failed_tables)} tables:")
                    for table, error in failed_tables:
                        logger.error(f"  - {table}: {error}")
                
                # Log summary statistics
                if pipeline.processed_metrics:
                    logger.info(f"📊 Total metrics processed: {len(pipeline.processed_metrics)}")
                    if pipeline.overwritten_metrics:
                        logger.info(f"📝 Metrics overwritten: {len(pipeline.overwritten_metrics)}")
                        logger.info(f"🆕 New metrics added: {len(pipeline.processed_metrics) - len(pipeline.overwritten_metrics)}")
                    else:
                        logger.info("🆕 All metrics were new (no existing metrics overwritten)")
                    
                    # Performance metrics
                    if execution_time.total_seconds() > 0:
                        metrics_per_second = len(pipeline.processed_metrics) / execution_time.total_seconds()
                        logger.info(f"⚡ Processing rate: {metrics_per_second:.2f} metrics/second")
                else:
                    logger.warning("⚠️ No metrics were processed")
                
                if pipeline.errors_encountered > 0:
                    logger.warning(f"⚠️ {pipeline.errors_encountered} errors encountered during processing")
                
                # If we have failed tables, this is a partial failure
                if failed_tables:
                    if not successful_tables:
                        # Complete failure
                        raise MetricsPipelineError(f"All {len(failed_tables)} tables failed to process")
                    else:
                        # Partial failure
                        logger.warning(f"Pipeline completed with partial success: {len(successful_tables)} successful, {len(failed_tables)} failed")
                else:
                    logger.info("🎉 Pipeline completed successfully!")
        
    except NonRetryableError as e:
        logger.error(f"❌ Non-retryable pipeline error: {str(e)}")
        logger.error(f"Error code: {e.error_code}")
        logger.error(f"Error context: {e.context}")
        
        # Don't attempt rollback for non-retryable errors
        sys.exit(1)
        
    except RetryableError as e:
        logger.error(f"❌ Retryable pipeline error (all retries exhausted): {str(e)}")
        logger.error(f"Error code: {e.error_code}")
        logger.error(f"Error context: {e.context}")
        
        # Attempt rollback if we have processed metrics
        if pipeline and pipeline.processed_metrics and partition_dt:
            try:
                logger.info("🔄 Attempting to rollback processed metrics")
                pipeline.rollback_all_processed_metrics(partition_dt)
            except Exception as rollback_error:
                logger.error(f"❌ Rollback failed: {str(rollback_error)}")
        
        sys.exit(2)  # Different exit code for retryable errors
        
    except ResourceExhaustionError as e:
        logger.error(f"❌ Resource exhaustion error: {str(e)}")
        logger.error(f"Error code: {e.error_code}")
        logger.error(f"Error context: {e.context}")
        
        # Attempt rollback if we have processed metrics
        if pipeline and pipeline.processed_metrics and partition_dt:
            try:
                logger.info("🔄 Attempting to rollback processed metrics")
                pipeline.rollback_all_processed_metrics(partition_dt)
            except Exception as rollback_error:
                logger.error(f"❌ Rollback failed: {str(rollback_error)}")
        
        sys.exit(3)  # Different exit code for resource exhaustion
        
    except MetricsPipelineError as e:
        logger.error(f"❌ Pipeline error: {str(e)}")
        if hasattr(e, 'error_code'):
            logger.error(f"Error code: {e.error_code}")
        if hasattr(e, 'context'):
            logger.error(f"Error context: {e.context}")
        
        # Attempt rollback if we have processed metrics
        if pipeline and pipeline.processed_metrics and partition_dt:
            try:
                logger.info("🔄 Attempting to rollback processed metrics")
                pipeline.rollback_all_processed_metrics(partition_dt)
            except Exception as rollback_error:
                logger.error(f"❌ Rollback failed: {str(rollback_error)}")
        
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        
        # Attempt rollback if we have processed metrics
        if pipeline and pipeline.processed_metrics and partition_dt:
            try:
                logger.info("🔄 Attempting to rollback processed metrics")
                pipeline.rollback_all_processed_metrics(partition_dt)
            except Exception as rollback_error:
                logger.error(f"❌ Rollback failed: {str(rollback_error)}")
        
        sys.exit(4)  # Different exit code for unexpected errors
        
    finally:
        # Final cleanup and logging
        end_time = datetime.utcnow()
        total_time = end_time - start_time
        
        logger.info("=" * 80)
        logger.info("PIPELINE EXECUTION COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Start time: {start_time.isoformat()}")
        logger.info(f"End time: {end_time.isoformat()}")
        logger.info(f"Total execution time: {total_time}")
        
        if pipeline:
            logger.info(f"Final metrics processed: {len(pipeline.processed_metrics)}")
            logger.info(f"Final errors encountered: {pipeline.errors_encountered}")
        
        # Clean up any remaining resources
        if pipeline:
            try:
                pipeline.cleanup_temp_files()
            except Exception as e:
                logger.warning(f"Error during cleanup: {str(e)}")
        
        logger.info("Pipeline execution finished")


if __name__ == "__main__":
    main()
