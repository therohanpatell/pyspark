# Deployment Guide

This guide covers deployment strategies, environment setup, and best practices for running the PySpark BigQuery Metrics Pipeline in production.

## Table of Contents

- [Deployment Options](#deployment-options)
- [Environment Setup](#environment-setup)
- [Configuration Management](#configuration-management)
- [Production Deployment](#production-deployment)
- [Monitoring and Alerting](#monitoring-and-alerting)
- [Security](#security)
- [Scaling](#scaling)
- [Maintenance](#maintenance)

## Deployment Options

### 1. Google Cloud Dataproc

**Best for**: Production workloads with auto-scaling requirements

#### Setup Dataproc Cluster

```bash
# Create Dataproc cluster
gcloud dataproc clusters create metrics-pipeline-cluster \
    --region=us-central1 \
    --zone=us-central1-a \
    --master-machine-type=n1-standard-4 \
    --worker-machine-type=n1-standard-2 \
    --num-workers=3 \
    --image-version=2.0 \
    --enable-autoscaling \
    --max-workers=10 \
    --min-workers=2 \
    --initialization-actions=gs://your-bucket/setup-script.sh \
    --metadata=BUCKET_NAME=your-bucket \
    --scopes=https://www.googleapis.com/auth/cloud-platform
```

#### Submit Job to Dataproc

```bash
# Submit PySpark job
gcloud dataproc jobs submit pyspark \
    --cluster=metrics-pipeline-cluster \
    --region=us-central1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    --py-files=gs://your-bucket/pyspark.py \
    gs://your-bucket/pyspark.py \
    -- \
    --gcs_path=gs://your-bucket/config/metrics.json \
    --run_date=2024-01-15 \
    --dependencies=daily_metrics \
    --partition_info_table=project.dataset.partition_info
```

### 2. Google Cloud Run

**Best for**: Serverless execution with event-driven triggers

#### Dockerfile

```dockerfile
FROM python:3.9-slim

# Install Java (required for Spark)
RUN apt-get update && apt-get install -y \
    openjdk-11-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install Spark
RUN curl -L https://archive.apache.org/dist/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz | tar -xz -C /opt/
ENV SPARK_HOME=/opt/spark-3.2.0-bin-hadoop3.2
ENV PATH=$PATH:$SPARK_HOME/bin

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY pyspark.py .

# Set entrypoint
ENTRYPOINT ["python", "pyspark.py"]
```

#### Deploy to Cloud Run

```bash
# Build and push image
docker build -t gcr.io/your-project/metrics-pipeline .
docker push gcr.io/your-project/metrics-pipeline

# Deploy to Cloud Run
gcloud run deploy metrics-pipeline \
    --image gcr.io/your-project/metrics-pipeline \
    --region us-central1 \
    --memory 4Gi \
    --cpu 2 \
    --timeout 3600 \
    --concurrency 1 \
    --set-env-vars GOOGLE_APPLICATION_CREDENTIALS=/tmp/key.json
```

### 3. Google Cloud Functions

**Best for**: Lightweight, event-driven processing

#### Function Code

```python
import functions_framework
from google.cloud import pubsub_v1
import json
import subprocess
import os

@functions_framework.cloud_event
def trigger_metrics_pipeline(cloud_event):
    """Triggered by Pub/Sub message"""
    
    # Parse message
    pubsub_message = json.loads(cloud_event.data['message']['data'])
    
    # Extract parameters
    gcs_path = pubsub_message.get('gcs_path')
    run_date = pubsub_message.get('run_date')
    dependencies = pubsub_message.get('dependencies')
    partition_info_table = pubsub_message.get('partition_info_table')
    
    # Run pipeline
    cmd = [
        'python', 'pyspark.py',
        '--gcs_path', gcs_path,
        '--run_date', run_date,
        '--dependencies', dependencies,
        '--partition_info_table', partition_info_table
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"Pipeline failed: {result.stderr}")
    
    return {"status": "success", "output": result.stdout}
```

### 4. Kubernetes

**Best for**: Container orchestration with complex scheduling

#### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: metrics-pipeline
spec:
  replicas: 1
  selector:
    matchLabels:
      app: metrics-pipeline
  template:
    metadata:
      labels:
        app: metrics-pipeline
    spec:
      containers:
      - name: metrics-pipeline
        image: gcr.io/your-project/metrics-pipeline:latest
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
        env:
        - name: GOOGLE_APPLICATION_CREDENTIALS
          value: /var/secrets/google/key.json
        volumeMounts:
        - name: google-cloud-key
          mountPath: /var/secrets/google
      volumes:
      - name: google-cloud-key
        secret:
          secretName: google-cloud-key
```

#### CronJob for Scheduled Execution

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: daily-metrics-pipeline
spec:
  schedule: "0 6 * * *"  # Daily at 6 AM
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: metrics-pipeline
            image: gcr.io/your-project/metrics-pipeline:latest
            args:
            - --gcs_path=gs://your-bucket/config/daily_metrics.json
            - --run_date=$(date +%Y-%m-%d)
            - --dependencies=daily_metrics
            - --partition_info_table=project.dataset.partition_info
          restartPolicy: OnFailure
```

## Environment Setup

### 1. Development Environment

```bash
# Create development environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Set environment variables
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account-key.json"
export SPARK_HOME="/path/to/spark"
export GCP_PROJECT="your-project-id"

# Test installation
python -c "import pyspark; print('✅ PySpark installed')"
python -c "from google.cloud import bigquery; print('✅ BigQuery client installed')"
```

### 2. Production Environment

```bash
# Production environment setup script
#!/bin/bash
set -e

# Update system
apt-get update && apt-get upgrade -y

# Install Java
apt-get install -y openjdk-11-jre-headless

# Download and install Spark
SPARK_VERSION=3.2.0
HADOOP_VERSION=3.2
curl -L "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" | tar -xz -C /opt/
export SPARK_HOME="/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"
export PATH=$PATH:$SPARK_HOME/bin

# Install Python dependencies
pip3 install -r requirements.txt

# Download BigQuery connector
mkdir -p /opt/spark-jars
curl -L "https://storage.googleapis.com/hadoop-lib/bigquery/spark-bigquery-latest_2.12.jar" -o /opt/spark-jars/spark-bigquery-latest_2.12.jar

# Set permissions
chmod +x pyspark.py
```

## Configuration Management

### 1. Environment-Specific Configurations

#### config/development.json
```json
{
  "environment": "development",
  "gcp_project": "dev-project",
  "dataset": "dev_metrics",
  "partition_info_table": "dev-project.metadata.partition_info",
  "spark_config": {
    "spark.executor.memory": "1g",
    "spark.driver.memory": "1g"
  }
}
```

#### config/production.json
```json
{
  "environment": "production",
  "gcp_project": "prod-project",
  "dataset": "prod_metrics",
  "partition_info_table": "prod-project.metadata.partition_info",
  "spark_config": {
    "spark.executor.memory": "4g",
    "spark.driver.memory": "2g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true"
  }
}
```

### 2. Configuration Loading

```python
import json
import os

def load_config(env='production'):
    """Load environment-specific configuration"""
    config_path = f"config/{env}.json"
    
    if not os.path.exists(config_path):
        raise ValueError(f"Configuration file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Override with environment variables
    config['gcp_project'] = os.environ.get('GCP_PROJECT', config.get('gcp_project'))
    config['dataset'] = os.environ.get('DATASET', config.get('dataset'))
    
    return config
```

### 3. Secret Management

#### Using Google Secret Manager

```python
from google.cloud import secretmanager

def get_secret(secret_name, project_id):
    """Retrieve secret from Google Secret Manager"""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# Usage
db_password = get_secret("db-password", "your-project-id")
```

#### Using Kubernetes Secrets

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: pipeline-secrets
type: Opaque
stringData:
  service-account-key: |
    {
      "type": "service_account",
      "project_id": "your-project",
      ...
    }
```

## Production Deployment

### 1. CI/CD Pipeline

#### .github/workflows/deploy.yml

```yaml
name: Deploy Metrics Pipeline

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v2
    
    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: 3.9
    
    - name: Install dependencies
      run: |
        pip install -r requirements.txt
        pip install pytest
    
    - name: Run tests
      run: pytest tests/
    
    - name: Lint code
      run: |
        pip install flake8
        flake8 pyspark.py
  
  deploy:
    needs: test
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    
    steps:
    - uses: actions/checkout@v2
    
    - name: Set up Cloud SDK
      uses: google-github-actions/setup-gcloud@v0
      with:
        project_id: ${{ secrets.GCP_PROJECT }}
        service_account_key: ${{ secrets.GCP_SA_KEY }}
        export_default_credentials: true
    
    - name: Deploy to Dataproc
      run: |
        gsutil cp pyspark.py gs://your-bucket/
        gsutil cp config/production.json gs://your-bucket/config/
        gsutil cp requirements.txt gs://your-bucket/
```

### 2. Infrastructure as Code

#### Terraform Configuration

```hcl
# dataproc.tf
resource "google_dataproc_cluster" "metrics_pipeline" {
  name   = "metrics-pipeline-cluster"
  region = "us-central1"

  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = "n1-standard-4"
      disk_config {
        boot_disk_type    = "pd-standard"
        boot_disk_size_gb = 50
      }
    }

    worker_config {
      num_instances = 3
      machine_type  = "n1-standard-2"
      disk_config {
        boot_disk_type    = "pd-standard"
        boot_disk_size_gb = 50
      }
    }

    preemptible_worker_config {
      num_instances = 2
    }

    software_config {
      image_version = "2.0"
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
    }

    initialization_action {
      script      = "gs://your-bucket/setup-script.sh"
      timeout_sec = 500
    }
  }
}

# BigQuery tables
resource "google_bigquery_dataset" "metrics" {
  dataset_id = "metrics"
  location   = "US"
}

resource "google_bigquery_table" "revenue_metrics" {
  dataset_id = google_bigquery_dataset.metrics.dataset_id
  table_id   = "revenue_metrics"

  schema = file("schemas/revenue_metrics.json")

  time_partitioning {
    type  = "DAY"
    field = "business_data_date"
  }
}
```

### 3. Deployment Scripts

#### deploy.sh

```bash
#!/bin/bash
set -e

ENVIRONMENT=${1:-production}
CLUSTER_NAME="metrics-pipeline-cluster"
REGION="us-central1"
BUCKET="your-bucket"

echo "🚀 Deploying to $ENVIRONMENT environment..."

# Upload files to GCS
echo "📦 Uploading files to GCS..."
gsutil cp pyspark.py gs://$BUCKET/
gsutil cp -r config/ gs://$BUCKET/
gsutil cp requirements.txt gs://$BUCKET/

# Update cluster if needed
echo "🔄 Updating Dataproc cluster..."
gcloud dataproc clusters update $CLUSTER_NAME \
    --region=$REGION \
    --initialization-actions=gs://$BUCKET/setup-script.sh \
    --max-workers=10 \
    --min-workers=2

# Test deployment
echo "🧪 Testing deployment..."
gcloud dataproc jobs submit pyspark \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    gs://$BUCKET/pyspark.py \
    -- \
    --gcs_path=gs://$BUCKET/config/test_metrics.json \
    --run_date=$(date +%Y-%m-%d) \
    --dependencies=test_metrics \
    --partition_info_table=project.dataset.partition_info

echo "✅ Deployment completed successfully!"
```

## Monitoring and Alerting

### 1. Stackdriver Monitoring

```python
from google.cloud import monitoring_v3
from google.cloud import logging

def setup_monitoring():
    """Set up monitoring for the pipeline"""
    
    # Create custom metrics
    client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{PROJECT_ID}"
    
    descriptor = monitoring_v3.MetricDescriptor(
        type="custom.googleapis.com/metrics_pipeline/processed_metrics",
        metric_kind=monitoring_v3.MetricDescriptor.MetricKind.GAUGE,
        value_type=monitoring_v3.MetricDescriptor.ValueType.INT64,
        description="Number of processed metrics",
    )
    
    client.create_metric_descriptor(
        name=project_name, 
        metric_descriptor=descriptor
    )

def record_metric(metric_name, value, labels=None):
    """Record custom metric"""
    client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{PROJECT_ID}"
    
    series = monitoring_v3.TimeSeries()
    series.metric.type = f"custom.googleapis.com/metrics_pipeline/{metric_name}"
    series.resource.type = "global"
    
    if labels:
        for key, value in labels.items():
            series.metric.labels[key] = value
    
    now = time.time()
    seconds = int(now)
    nanos = int((now - seconds) * 10 ** 9)
    interval = monitoring_v3.TimeInterval(
        {"end_time": {"seconds": seconds, "nanos": nanos}}
    )
    
    point = monitoring_v3.Point(
        {"interval": interval, "value": {"int64_value": value}}
    )
    series.points = [point]
    
    client.create_time_series(name=project_name, time_series=[series])
```

### 2. Alerting Policies

```yaml
# alerting-policy.yaml
displayName: "Metrics Pipeline Failures"
conditions:
  - displayName: "Pipeline Error Rate"
    conditionThreshold:
      filter: 'resource.type="gce_instance" AND logName="projects/your-project/logs/metrics-pipeline"'
      comparison: COMPARISON_GREATER_THAN
      thresholdValue: 0.1
      duration: 300s
    
notificationChannels:
  - "projects/your-project/notificationChannels/email-alerts"
  - "projects/your-project/notificationChannels/slack-alerts"

alertStrategy:
  autoClose: 604800s  # 7 days
```

### 3. Health Check Endpoint

```python
from flask import Flask, jsonify
import threading
import time

app = Flask(__name__)

class HealthChecker:
    def __init__(self):
        self.last_successful_run = None
        self.current_status = "unknown"
        
    def update_status(self, status, timestamp=None):
        self.current_status = status
        if status == "success":
            self.last_successful_run = timestamp or time.time()

health_checker = HealthChecker()

@app.route('/health')
def health_check():
    """Health check endpoint"""
    current_time = time.time()
    
    # Check if last successful run was within acceptable window
    if health_checker.last_successful_run:
        time_since_last_success = current_time - health_checker.last_successful_run
        if time_since_last_success > 86400:  # 24 hours
            return jsonify({
                "status": "unhealthy",
                "message": "No successful runs in the last 24 hours"
            }), 503
    
    return jsonify({
        "status": "healthy",
        "last_successful_run": health_checker.last_successful_run,
        "current_status": health_checker.current_status
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
```

## Security

### 1. Service Account Setup

```bash
# Create service account
gcloud iam service-accounts create metrics-pipeline-sa \
    --display-name="Metrics Pipeline Service Account"

# Grant required permissions
gcloud projects add-iam-policy-binding your-project \
    --member="serviceAccount:metrics-pipeline-sa@your-project.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding your-project \
    --member="serviceAccount:metrics-pipeline-sa@your-project.iam.gserviceaccount.com" \
    --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding your-project \
    --member="serviceAccount:metrics-pipeline-sa@your-project.iam.gserviceaccount.com" \
    --role="roles/storage.objectViewer"

# Create and download key
gcloud iam service-accounts keys create service-account-key.json \
    --iam-account=metrics-pipeline-sa@your-project.iam.gserviceaccount.com
```

### 2. Network Security

```hcl
# VPC configuration
resource "google_compute_network" "metrics_pipeline_vpc" {
  name                    = "metrics-pipeline-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "metrics_pipeline_subnet" {
  name          = "metrics-pipeline-subnet"
  network       = google_compute_network.metrics_pipeline_vpc.id
  ip_cidr_range = "10.0.0.0/24"
  region        = "us-central1"
}

# Firewall rules
resource "google_compute_firewall" "metrics_pipeline_firewall" {
  name    = "metrics-pipeline-firewall"
  network = google_compute_network.metrics_pipeline_vpc.name

  allow {
    protocol = "tcp"
    ports    = ["8080", "8081"]
  }

  source_ranges = ["10.0.0.0/24"]
  target_tags   = ["metrics-pipeline"]
}
```

### 3. Data Encryption

```python
from google.cloud import kms

def encrypt_sensitive_data(data, key_name):
    """Encrypt sensitive data using Cloud KMS"""
    client = kms.KeyManagementServiceClient()
    
    key_name = client.crypto_key_path(
        PROJECT_ID, 
        "us-central1", 
        "metrics-pipeline-ring", 
        key_name
    )
    
    response = client.encrypt(
        request={"name": key_name, "plaintext": data.encode()}
    )
    
    return response.ciphertext

def decrypt_sensitive_data(ciphertext, key_name):
    """Decrypt sensitive data using Cloud KMS"""
    client = kms.KeyManagementServiceClient()
    
    key_name = client.crypto_key_path(
        PROJECT_ID, 
        "us-central1", 
        "metrics-pipeline-ring", 
        key_name
    )
    
    response = client.decrypt(
        request={"name": key_name, "ciphertext": ciphertext}
    )
    
    return response.plaintext.decode()
```

## Scaling

### 1. Auto-scaling Configuration

```bash
# Create cluster with auto-scaling
gcloud dataproc clusters create metrics-pipeline-cluster \
    --enable-autoscaling \
    --max-workers=20 \
    --min-workers=3 \
    --secondary-worker-type=preemptible \
    --num-preemptible-workers=5 \
    --preemptible-worker-boot-disk-size=50GB
```

### 2. Horizontal Scaling

```python
def scale_processing(metrics_count):
    """Dynamically scale processing based on metrics count"""
    
    if metrics_count < 100:
        return {
            "executor_instances": 2,
            "executor_memory": "2g",
            "driver_memory": "1g"
        }
    elif metrics_count < 1000:
        return {
            "executor_instances": 5,
            "executor_memory": "4g",
            "driver_memory": "2g"
        }
    else:
        return {
            "executor_instances": 10,
            "executor_memory": "8g",
            "driver_memory": "4g"
        }
```

### 3. Resource Optimization

```python
# Optimize Spark configuration based on data size
def optimize_spark_config(data_size_gb):
    """Optimize Spark configuration based on data size"""
    
    base_config = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
    }
    
    if data_size_gb > 10:
        base_config.update({
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "256MB"
        })
    
    return base_config
```

## Maintenance

### 1. Backup Strategy

```bash
#!/bin/bash
# backup.sh

DATE=$(date +%Y%m%d)
BACKUP_BUCKET="your-backup-bucket"

# Backup configurations
gsutil cp -r gs://your-bucket/config/ gs://$BACKUP_BUCKET/backups/$DATE/config/

# Backup BigQuery tables
bq extract --destination_format=AVRO \
    project:dataset.metrics_table \
    gs://$BACKUP_BUCKET/backups/$DATE/metrics_table_*.avro
```

### 2. Log Rotation

```python
import logging
from logging.handlers import RotatingFileHandler

def setup_logging():
    """Set up log rotation"""
    
    # Create rotating file handler
    handler = RotatingFileHandler(
        'metrics_pipeline.log',
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    
    # Set format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    
    # Add handler to logger
    logger = logging.getLogger()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
```

### 3. Cleanup Scripts

```bash
#!/bin/bash
# cleanup.sh

# Clean up old Dataproc clusters
gcloud dataproc clusters list --filter="status.state:RUNNING" --format="value(clusterName)" | \
while read cluster; do
    CREATION_TIME=$(gcloud dataproc clusters describe $cluster --format="value(status.stateStartTime)")
    # Add logic to delete old clusters
done

# Clean up old BigQuery jobs
bq ls -j --max_results=1000 --format=json | \
jq -r '.[] | select(.statistics.creationTime < "'"$(date -d '7 days ago' +%s)"'000") | .jobReference.jobId' | \
while read job_id; do
    echo "Cleaning up job: $job_id"
    # Jobs are automatically cleaned up by BigQuery
done
```

### 4. Update Procedures

```bash
#!/bin/bash
# update.sh

ENVIRONMENT=${1:-production}
echo "🔄 Updating $ENVIRONMENT environment..."

# Backup current version
gsutil cp gs://your-bucket/pyspark.py gs://your-bucket/backups/pyspark_$(date +%Y%m%d).py

# Deploy new version
gsutil cp pyspark.py gs://your-bucket/

# Test new version
python test_pipeline.py --environment=$ENVIRONMENT

# Rollback if needed
if [ $? -ne 0 ]; then
    echo "❌ Update failed, rolling back..."
    gsutil cp gs://your-bucket/backups/pyspark_$(date +%Y%m%d).py gs://your-bucket/pyspark.py
    exit 1
fi

echo "✅ Update completed successfully!"
```

This deployment guide provides comprehensive coverage of different deployment scenarios, from development to production, with emphasis on security, monitoring, and maintenance best practices.