#!/usr/bin/env python3
"""
Usage Examples for PySpark BigQuery Metrics Pipeline

This file demonstrates various ways to use the pipeline programmatically
with different scenarios and configurations.
"""

from pyspark.sql import SparkSession
from google.cloud import bigquery
from datetime import datetime, timedelta
import logging
import json

# Assuming the pipeline module is in the same directory
from pyspark import MetricsPipeline, MetricsPipelineError, managed_spark_session

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def example_1_basic_usage():
    """
    Example 1: Basic pipeline usage with single dependency
    """
    print("=" * 60)
    print("Example 1: Basic Pipeline Usage")
    print("=" * 60)
    
    try:
        with managed_spark_session("Example1") as spark:
            # Initialize BigQuery client
            bq_client = bigquery.Client()
            
            # Create pipeline instance
            pipeline = MetricsPipeline(spark, bq_client)
            
            # Configuration
            gcs_path = "gs://your-bucket/config/daily_metrics.json"
            run_date = "2024-01-15"
            dependencies = ["daily_metrics"]
            partition_info_table = "project.dataset.partition_info"
            
            # Execute pipeline
            json_data = pipeline.read_json_from_gcs(gcs_path)
            validated_data = pipeline.validate_json(json_data)
            metrics_dfs = pipeline.process_metrics(
                validated_data, 
                run_date, 
                dependencies, 
                partition_info_table
            )
            
            # Write to BigQuery
            for target_table, df in metrics_dfs.items():
                aligned_df = pipeline.align_schema_with_bq(df, target_table)
                pipeline.write_to_bq_with_overwrite(aligned_df, target_table)
            
            print(f"✅ Successfully processed {len(pipeline.processed_metrics)} metrics")
            
    except MetricsPipelineError as e:
        logger.error(f"Pipeline error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def example_2_multiple_dependencies():
    """
    Example 2: Processing multiple dependencies in sequence
    """
    print("=" * 60)
    print("Example 2: Multiple Dependencies")
    print("=" * 60)
    
    try:
        with managed_spark_session("Example2") as spark:
            bq_client = bigquery.Client()
            pipeline = MetricsPipeline(spark, bq_client)
            
            # Configuration
            gcs_path = "gs://your-bucket/config/all_metrics.json"
            run_date = "2024-01-15"
            dependencies = ["daily_metrics", "weekly_metrics", "monthly_metrics"]
            partition_info_table = "project.dataset.partition_info"
            
            # Read and validate once
            json_data = pipeline.read_json_from_gcs(gcs_path)
            validated_data = pipeline.validate_json(json_data)
            
            # Process each dependency group
            for dependency in dependencies:
                print(f"Processing dependency: {dependency}")
                
                try:
                    metrics_dfs = pipeline.process_metrics(
                        validated_data, 
                        run_date, 
                        [dependency],  # Process one dependency at a time
                        partition_info_table
                    )
                    
                    # Write to BigQuery
                    for target_table, df in metrics_dfs.items():
                        aligned_df = pipeline.align_schema_with_bq(df, target_table)
                        pipeline.write_to_bq_with_overwrite(aligned_df, target_table)
                    
                    print(f"✅ Completed dependency: {dependency}")
                    
                except MetricsPipelineError as e:
                    logger.error(f"Error processing {dependency}: {e}")
                    continue
            
            print(f"✅ Total metrics processed: {len(pipeline.processed_metrics)}")
            
    except Exception as e:
        logger.error(f"Error in multiple dependencies example: {e}")


def example_3_error_handling_and_rollback():
    """
    Example 3: Demonstrating error handling and rollback functionality
    """
    print("=" * 60)
    print("Example 3: Error Handling and Rollback")
    print("=" * 60)
    
    partition_dt = datetime.now().strftime('%Y-%m-%d')
    pipeline = None
    
    try:
        with managed_spark_session("Example3") as spark:
            bq_client = bigquery.Client()
            pipeline = MetricsPipeline(spark, bq_client)
            
            # Configuration
            gcs_path = "gs://your-bucket/config/risky_metrics.json"
            run_date = "2024-01-15"
            dependencies = ["daily_metrics"]
            partition_info_table = "project.dataset.partition_info"
            
            # Execute pipeline with error handling
            json_data = pipeline.read_json_from_gcs(gcs_path)
            validated_data = pipeline.validate_json(json_data)
            metrics_dfs = pipeline.process_metrics(
                validated_data, 
                run_date, 
                dependencies, 
                partition_info_table
            )
            
            # Simulate processing with potential failure
            for target_table, df in metrics_dfs.items():
                try:
                    aligned_df = pipeline.align_schema_with_bq(df, target_table)
                    pipeline.write_to_bq_with_overwrite(aligned_df, target_table)
                    print(f"✅ Successfully wrote to {target_table}")
                    
                except Exception as e:
                    logger.error(f"Failed to write to {target_table}: {e}")
                    # Trigger rollback
                    raise MetricsPipelineError(f"Write failed for {target_table}: {e}")
            
            print("✅ All metrics processed successfully")
            
    except MetricsPipelineError as e:
        logger.error(f"Pipeline error occurred: {e}")
        
        # Perform rollback
        if pipeline and pipeline.processed_metrics:
            logger.info("Attempting to rollback processed metrics...")
            try:
                pipeline.rollback_all_processed_metrics(partition_dt)
                logger.info("✅ Rollback completed successfully")
            except Exception as rollback_error:
                logger.error(f"❌ Rollback failed: {rollback_error}")
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def example_4_custom_validation():
    """
    Example 4: Custom validation logic for business rules
    """
    print("=" * 60)
    print("Example 4: Custom Validation")
    print("=" * 60)
    
    def validate_business_rules(json_data):
        """Custom validation for business-specific rules"""
        for record in json_data:
            # Rule 1: Currency metrics must use SUM or AVG
            if record['metric_type'] == 'currency':
                sql_upper = record['sql'].upper()
                if 'SUM(' not in sql_upper and 'AVG(' not in sql_upper:
                    raise MetricsPipelineError(
                        f"Currency metric '{record['metric_id']}' must use SUM or AVG aggregation"
                    )
            
            # Rule 2: Percentage metrics must have numerator and denominator
            if record['metric_type'] == 'percentage':
                if 'numerator_value' not in record['sql'] or 'denominator_value' not in record['sql']:
                    raise MetricsPipelineError(
                        f"Percentage metric '{record['metric_id']}' must have numerator_value and denominator_value"
                    )
            
            # Rule 3: Count metrics should use COUNT function
            if record['metric_type'] == 'count':
                if 'COUNT(' not in record['sql'].upper():
                    logger.warning(f"Count metric '{record['metric_id']}' doesn't use COUNT function")
        
        return json_data
    
    try:
        with managed_spark_session("Example4") as spark:
            bq_client = bigquery.Client()
            pipeline = MetricsPipeline(spark, bq_client)
            
            # Configuration
            gcs_path = "gs://your-bucket/config/validated_metrics.json"
            run_date = "2024-01-15"
            dependencies = ["daily_metrics"]
            partition_info_table = "project.dataset.partition_info"
            
            # Execute with custom validation
            json_data = pipeline.read_json_from_gcs(gcs_path)
            validated_data = pipeline.validate_json(json_data)
            
            # Apply custom business rules
            business_validated_data = validate_business_rules(validated_data)
            
            # Continue with processing
            metrics_dfs = pipeline.process_metrics(
                business_validated_data, 
                run_date, 
                dependencies, 
                partition_info_table
            )
            
            print(f"✅ Custom validation passed for {len(metrics_dfs)} target tables")
            
    except MetricsPipelineError as e:
        logger.error(f"Custom validation failed: {e}")
    except Exception as e:
        logger.error(f"Error in custom validation example: {e}")


def example_5_date_range_processing():
    """
    Example 5: Processing metrics for a date range
    """
    print("=" * 60)
    print("Example 5: Date Range Processing")
    print("=" * 60)
    
    def process_date_range(start_date, end_date, gcs_path, dependencies, partition_info_table):
        """Process metrics for a range of dates"""
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        results = {}
        
        while current_date <= end_date_dt:
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"Processing date: {date_str}")
            
            try:
                with managed_spark_session(f"DateRange_{date_str}") as spark:
                    bq_client = bigquery.Client()
                    pipeline = MetricsPipeline(spark, bq_client)
                    
                    # Process metrics for this date
                    json_data = pipeline.read_json_from_gcs(gcs_path)
                    validated_data = pipeline.validate_json(json_data)
                    metrics_dfs = pipeline.process_metrics(
                        validated_data, 
                        date_str, 
                        dependencies, 
                        partition_info_table
                    )
                    
                    # Write to BigQuery
                    for target_table, df in metrics_dfs.items():
                        aligned_df = pipeline.align_schema_with_bq(df, target_table)
                        pipeline.write_to_bq_with_overwrite(aligned_df, target_table)
                    
                    results[date_str] = len(pipeline.processed_metrics)
                    print(f"✅ Processed {len(pipeline.processed_metrics)} metrics for {date_str}")
                    
            except Exception as e:
                logger.error(f"Error processing {date_str}: {e}")
                results[date_str] = f"Error: {str(e)}"
            
            current_date += timedelta(days=1)
        
        return results
    
    # Process last 7 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=6)
    
    results = process_date_range(
        start_date.strftime('%Y-%m-%d'),
        end_date.strftime('%Y-%m-%d'),
        "gs://your-bucket/config/daily_metrics.json",
        ["daily_metrics"],
        "project.dataset.partition_info"
    )
    
    print("\n📊 Date Range Processing Results:")
    for date, result in results.items():
        print(f"  {date}: {result}")


def example_6_monitoring_and_alerting():
    """
    Example 6: Adding monitoring and alerting capabilities
    """
    print("=" * 60)
    print("Example 6: Monitoring and Alerting")
    print("=" * 60)
    
    class MetricsMonitor:
        def __init__(self):
            self.alerts = []
            self.metrics_summary = {}
        
        def check_metric_value(self, metric_id, metric_value, metric_type):
            """Check if metric value is within expected ranges"""
            try:
                value = float(metric_value) if metric_value else 0
                
                # Define thresholds (in practice, these would come from configuration)
                thresholds = {
                    'daily_revenue': {'min': 1000, 'max': 100000},
                    'conversion_rate': {'min': 1.0, 'max': 15.0},
                    'bounce_rate': {'min': 20.0, 'max': 80.0}
                }
                
                if metric_id in thresholds:
                    threshold = thresholds[metric_id]
                    if value < threshold['min']:
                        self.alerts.append(f"🚨 {metric_id} below threshold: {value} < {threshold['min']}")
                    elif value > threshold['max']:
                        self.alerts.append(f"🚨 {metric_id} above threshold: {value} > {threshold['max']}")
                    else:
                        self.alerts.append(f"✅ {metric_id} within normal range: {value}")
                
                self.metrics_summary[metric_id] = {
                    'value': value,
                    'type': metric_type,
                    'status': 'normal' if metric_id not in [alert for alert in self.alerts if '🚨' in alert] else 'alert'
                }
                
            except (ValueError, TypeError):
                self.alerts.append(f"❌ Invalid metric value for {metric_id}: {metric_value}")
        
        def send_alerts(self):
            """Send alerts (in practice, this would integrate with alerting systems)"""
            if self.alerts:
                print("\n📢 Monitoring Alerts:")
                for alert in self.alerts:
                    print(f"  {alert}")
            else:
                print("\n✅ No alerts - all metrics within normal ranges")
    
    try:
        with managed_spark_session("Example6") as spark:
            bq_client = bigquery.Client()
            pipeline = MetricsPipeline(spark, bq_client)
            monitor = MetricsMonitor()
            
            # Configuration
            gcs_path = "gs://your-bucket/config/monitored_metrics.json"
            run_date = "2024-01-15"
            dependencies = ["daily_metrics"]
            partition_info_table = "project.dataset.partition_info"
            
            # Execute pipeline with monitoring
            json_data = pipeline.read_json_from_gcs(gcs_path)
            validated_data = pipeline.validate_json(json_data)
            
            # Process metrics with monitoring
            for record in validated_data:
                if record['dependency'] in dependencies:
                    try:
                        # Execute SQL and get result
                        sql_result = pipeline.execute_sql(
                            record['sql'],
                            run_date,
                            partition_info_table,
                            record['metric_id']
                        )
                        
                        # Monitor the metric value
                        monitor.check_metric_value(
                            record['metric_id'],
                            sql_result['metric_output'],
                            record['metric_type']
                        )
                        
                    except Exception as e:
                        logger.error(f"Error processing metric {record['metric_id']}: {e}")
            
            # Send alerts
            monitor.send_alerts()
            
            # Print summary
            print(f"\n📊 Metrics Summary:")
            for metric_id, summary in monitor.metrics_summary.items():
                print(f"  {metric_id}: {summary['value']} ({summary['status']})")
            
    except Exception as e:
        logger.error(f"Error in monitoring example: {e}")


def main():
    """Run all examples"""
    print("🚀 PySpark BigQuery Metrics Pipeline - Usage Examples")
    print("=" * 60)
    
    examples = [
        example_1_basic_usage,
        example_2_multiple_dependencies,
        example_3_error_handling_and_rollback,
        example_4_custom_validation,
        example_5_date_range_processing,
        example_6_monitoring_and_alerting
    ]
    
    for i, example in enumerate(examples, 1):
        try:
            print(f"\n🔄 Running Example {i}...")
            example()
            print(f"✅ Example {i} completed successfully")
        except Exception as e:
            print(f"❌ Example {i} failed: {e}")
        
        print("\n" + "=" * 60)
    
    print("🎉 All examples completed!")


if __name__ == "__main__":
    main()