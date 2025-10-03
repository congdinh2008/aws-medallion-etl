# Airflow + PySpark Data Pipeline Project

## Tổng quan

Đây là một data pipeline hoàn chỉnh theo **Medallion Architecture** (Bronze → Silver → Gold) để xử lý dữ liệu Meta Kaggle, sử dụng Apache Airflow cho orchestration và Apache Spark cluster cho data processing trong môi trường containerized.

### Kiến trúc chính

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Airflow Web    │    │ Airflow Worker  │    │   PostgreSQL    │
│     (8080)      │◄──►│   + Celery      │◄──►│     (5432)      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         ▲                       ▲                       ▲
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │      Redis      │
                    │     (6379)      │
                    └─────────────────┘

┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Spark Master  │    │  Spark Worker   │    │ History Server  │
│     (1990)      │◄──►│     (8081)      │    │     (18080)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         ▲                       ▲                       ▲
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │   JupyterLab    │
                    │     (8888)      │
                    └─────────────────┘
```

**Thành phần chính:**
- **Airflow**: Orchestration với CeleryExecutor, Redis, PostgreSQL
- **Spark Cluster**: Master, Worker, History Server (PySpark 3.5.0)
- **Storage**: Parquet format theo Medallion Architecture
- **Data Source**: Meta Kaggle datasets (CSV format)
- **Development**: JupyterLab với PySpark integration

**Tính năng nổi bật:**
- 🚀 **Spark 3.5.0** với cluster setup hoàn chỉnh
- 📊 **JupyterLab** với PySpark kernel được cấu hình sẵn
- 📈 **History Server** cho monitoring ứng dụng
- 🔧 **Auto-scaling** worker nodes
- 💾 **Persistent storage** cho data và notebooks
- 🎯 **Health checks** cho tất cả services
- ⚙️ **Configuration-driven** qua YAML và environment variables

## Cấu trúc thư mục

```
├── docker-compose.yaml         # Service definitions chính
├── .env                       # Environment configuration
├── .env.example              # Environment template
├── dags/                     # Airflow DAGs
│   └── basic/               # Example DAGs
│       ├── 01_hello_world_dag.py
│       ├── 02_file_processing_dag.py
│       └── 03_file_processing_v2_dag.py
├── jobs/                     # PySpark job scripts
│   └── meta/                # Meta Kaggle processing jobs
├── libs/                     # Reusable Python utilities
├── configs/                  # YAML configuration files
├── data/                     # Data storage (mounted in containers)
│   └── meta/
│       ├── raw/             # CSV source files
│       ├── bronze/          # Raw + metadata (Parquet)
│       ├── silver/          # Cleaned data (Parquet)
│       └── gold/            # Business metrics (Parquet)
├── conf/                     # Spark configuration files
│   ├── spark-defaults.conf   # Main Spark configuration
│   ├── spark-defaults.client.conf # Client-specific settings
│   ├── log4j2.properties    # Logging configuration
│   └── metrics.properties   # Metrics configuration
├── images/                   # Docker image definitions
│   ├── airflow-custom/       # Custom Airflow image
│   │   ├── Dockerfile        # Airflow với PySpark dependencies
│   │   └── requirements.txt  # Python packages
│   └── custom-notebook/      # Custom Jupyter image
│       ├── docker-bake.hcl   # Docker Buildx Bake configuration
│       ├── Dockerfile        # Custom Jupyter với Spark integration
│       ├── setup_spark.py    # Spark installation script
│       └── ipython_kernel_config.py # IPython kernel configuration
├── notebooks/                # Jupyter notebooks
├── events/                   # Spark event logs (for History Server)
├── logs/                     # Airflow logs
├── plugins/                  # Airflow plugins
├── secrets/                  # Secrets và credentials
│   └── kaggle/              # Kaggle API credentials
├── terraform/                # Infrastructure as Code
│   ├── environments/        # Environment-specific configs
│   └── modules/             # Reusable Terraform modules
├── boto3_deployment/         # AWS deployment utilities
└── requirements/             # Documentation & requirements
    └── meta/                # Meta pipeline requirements
```

## Data Pipeline Layers

### 🥉 Bronze Layer
- **Input**: Raw CSV files từ Meta Kaggle
- **Processing**: Schema validation, metadata addition
- **Output**: Parquet files với `ingest_ts`, `file_name`, `source_system`
- **Partitioning**: Theo `source_table`

### 🥈 Silver Layer
- **Processing**: Data cleaning, deduplication, standardization
- **Features**: Window functions, join enrichment, missing data handling
- **Quality**: Data validation với rejection patterns

### 🥇 Gold Layer
- **Processing**: Business metrics, dimensional modeling
- **Features**: KPI tables, SCD Type 2, fact tables
- **Output**: Ready-to-use data cho BI và analytics

## Yêu cầu hệ thống

### Phần mềm bắt buộc
- **Docker** và **Docker Compose** (phiên bản mới nhất)
- **Docker Buildx plugin** (cho custom image building)
- **Python 3.8+** (cho local development)
- Java 11 JRE (được cài tự động trong container)

### Tài nguyên hệ thống
- **RAM**: Tối thiểu 8GB (khuyến nghị 16GB+ cho môi trường production)
- **CPU**: Tối thiểu 4 cores (khuyến nghị 8+ cores)
- **Disk**: Tối thiểu 20GB free space
- **Ports**: 7077, 1990, 8080, 8888, 18080, 5432, 6379 phải available

## Docker Setup và Build Options

Dự án này hỗ trợ nhiều phương pháp deployment Docker để phù hợp với các use case và môi trường khác nhau.

### Phương pháp 1: Quick Start với Pre-built Images

Cách nhanh nhất để bắt đầu là sử dụng docker-compose setup mặc định:

```bash
# Clone repository
git clone <repository-url>
cd aws-medallion-etl

# Khởi động với pre-built images
docker-compose up -d
```

### Phương pháp 2: Building Custom Images

Để tùy chỉnh hoặc khi pre-built image không available:

```bash
# Build custom images và khởi động services
docker-compose up -d --build
```

Điều này sẽ:
1. Build custom Airflow image với PySpark dependencies
2. Build custom Jupyter image với Spark integration  
3. Khởi động tất cả services (airflow, spark cluster, jupyter)

### Phương pháp 3: Sử dụng Docker Buildx Bake (Advanced)

Để có control nhiều hơn về build process, dự án bao gồm file `docker-bake.hcl` configuration:

```bash
# Build custom Jupyter image bằng Docker Buildx Bake
cd images/custom-notebook
docker buildx bake

# Hoặc build specific targets
docker buildx bake custom-notebook

# Khởi động services sau khi build
cd ../..
docker-compose up -d
```

#### Hiểu về docker-bake.hcl

File `docker-bake.hcl` định nghĩa multi-stage build process:

```hcl
target "foundation" -> target "base-notebook" -> target "minimal-notebook" -> target "custom-notebook"
```

- **foundation**: Base Python 3.10.12 environment
- **base-notebook**: Basic Jupyter setup từ official Docker Stacks
- **minimal-notebook**: Minimal Jupyter environment với essential packages
- **custom-notebook**: Customized image với Spark integration

**Lợi ích của Docker Buildx Bake:**
- ⚡ **Parallel builds**: Multiple targets được build đồng thời
- 🔧 **Layer caching**: Efficient rebuilds với dependency tracking
- 📦 **Official base images**: Built trên trusted Jupyter Docker Stacks
- 🎯 **Targeted builds**: Build chỉ những gì cần thiết

### Environment Configuration

```bash
# Tạo environment file
cp .env.example .env

# Chỉnh sửa configuration theo nhu cầu
nano .env
```

**Key environment variables:**
```bash
# Image building
SPARK_VERSION=3.5.0
JUPYTERLAB_VERSION=latest

# Service ports
SPARK_MASTER_WEBUI_PORT=1990
JUPYTER_PORT=8888
SPARK_HISTORY_SERVER_PORT=18080

# Resource allocation
SPARK_WORKER_MEMORY=16G
SPARK_WORKER_CORES=8
SPARK_WORKER_REPLICAS=1
```

### Chuẩn bị dữ liệu Meta Kaggle

Tải xuống các file CSV từ Kaggle Meta dataset và đặt vào `data/meta/raw/`:
- `Datasets.csv`
- `Competitions.csv` 
- `Users.csv`
- `Tags.csv`
- `Kernels.csv`

### Quick Start Steps

```bash
# 1. Clone repository
git clone <repository-url>
cd aws-medallion-etl

# 2. Setup environment
cp .env.example .env

# 3. Tạo thư mục cần thiết
mkdir -p data/meta/{raw,bronze,silver,gold}
mkdir -p configs jobs/meta libs tests

# 4. Choose deployment method:

# Option A: Quick start (sử dụng pre-built hoặc build tự động)
docker-compose up -d

# Option B: Force rebuild custom images
docker-compose up -d --build

# Option C: Sử dụng Docker Buildx Bake (khuyến nghị cho development)
cd images/custom-notebook && docker buildx bake && cd ../..
docker-compose up -d
```

### Truy cập các Services

| Service | URL | Description | Credentials |
|---------|-----|-------------|-------------|
| **Airflow Webserver** | http://localhost:8080 | DAG management và monitoring | airflow/airflow |
| **Spark Master UI** | http://localhost:1990 | Cluster management và monitoring | - |
| **Spark History Server** | http://localhost:18080 | Completed applications history | - |
| **JupyterLab** | http://localhost:8888 | Interactive development environment | Token: psi |
| **Flower (Celery)** | http://localhost:5555 | Celery task monitoring | Cần `--profile flower` |

### Verification

```bash
# Kiểm tra tất cả services đang chạy
docker-compose ps

# Xem logs tổng quát
docker-compose logs -f

# Xem logs của service cụ thể
docker-compose logs -f airflow-scheduler
docker-compose logs -f spark-master
```

## Usage Examples và Development Workflow

### Chạy Sample Applications

Dự án bao gồm các sample PySpark applications trong `dags/basic/`:

#### 1. Hello World DAG
```bash
# Kích hoạt DAG từ Airflow UI hoặc CLI
docker-compose exec airflow-worker airflow dags trigger 01_hello_world_dag
```

#### 2. File Processing DAG
```bash
# Submit file processing job
docker-compose exec airflow-worker airflow dags trigger 02_file_processing_dag
```

#### 3. Advanced File Processing
```bash
# Submit advanced processing job
docker-compose exec airflow-worker airflow dags trigger 03_file_processing_v2_dag
```

### Sử dụng JupyterLab cho Interactive Development

1. **Truy cập JupyterLab**: http://localhost:8888 (token: psi)
2. **Navigate** đến thư mục `work` directory
3. **Tạo notebook mới** hoặc mở existing notebooks
4. **PySpark được cấu hình sẵn** và ready to use:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# SparkSession tự động kết nối đến cluster
spark = SparkSession.builder \
    .appName("MetaDataAnalysis") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()

# Example: Load Meta Kaggle datasets
datasets_df = spark.read.option("header", "true") \
    .csv("/opt/airflow/data/meta/raw/Datasets.csv")

datasets_df.show(10)
datasets_df.printSchema()

# Perform some analysis
popular_datasets = datasets_df.groupBy("OwnerUserId") \
    .count() \
    .orderBy(desc("count")) \
    .limit(10)

popular_datasets.show()
```

### Development Workflow

#### 1. Interactive Development Phase
```python
# Trong JupyterLab - Phát triển và test logic
from pyspark.sql import SparkSession
import yaml

# Load configuration
with open('/opt/airflow/configs/meta_pipeline_config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Test transformations interactively
spark = SparkSession.builder.appName("Development").getOrCreate()
# Your development code here...
```

#### 2. PySpark Job Development
```python
# Tạo PySpark jobs trong jobs/meta/
# jobs/meta/bronze_processing.py

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import yaml
import sys

def main():
    spark = SparkSession.builder \
        .appName("BronzeProcessing") \
        .getOrCreate()
    
    # Load config
    with open('/opt/airflow/configs/meta_pipeline_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Processing logic
    # ...
    
    spark.stop()

if __name__ == "__main__":
    main()
```

#### 3. Airflow DAG Development

```python
# dags/meta_pipeline_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta

# Luôn sử dụng configuration-driven approach
base_dir = Variable.get("BASE_DATA_DIR", default_var="/opt/airflow/data")

def run_spark_job(**context):
    """Submit PySpark job to cluster"""
    import subprocess
    
    cmd = [
        "spark-submit",
        "--master", "spark://spark-master:7077",
        "--deploy-mode", "client",
        "/opt/airflow/jobs/meta/bronze_processing.py"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Spark job failed: {result.stderr}")
    
    return result.stdout

# DAG definition
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'meta_pipeline',
    default_args=default_args,
    description='Meta Kaggle Data Pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['meta', 'kaggle', 'medallion']
) as dag:

    # Bronze layer processing
    bronze_task = PythonOperator(
        task_id='bronze_processing',
        python_callable=run_spark_job
    )
    
    # Silver layer processing  
    silver_task = BashOperator(
        task_id='silver_processing',
        bash_command="""
        spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        /opt/airflow/jobs/meta/silver_processing.py
        """
    )
    
    # Task dependencies
    bronze_task >> silver_task
```

### Testing và Debugging

#### Unit Testing
```bash
# Chạy unit tests trong container
docker-compose exec airflow-worker bash -c "cd /opt/airflow && python -m pytest tests/ -v"

# Test PySpark jobs với chispa
docker-compose exec airflow-worker bash -c """
cd /opt/airflow && python -c '
from chispa import assert_df_equality
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Test").getOrCreate()
# Your test code here
'
"""
```

#### Debugging và Monitoring

```bash
# Xem logs của các services
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-worker  
docker-compose logs -f spark-master
docker-compose logs -f spark-worker

# Access vào container để debug
docker-compose exec airflow-worker bash
docker-compose exec spark-master bash

# Kiểm tra Spark applications
curl http://localhost:1990/api/v1/applications
```

#### Performance Monitoring

1. **Live Applications**: http://localhost:1990 - Monitor running jobs
2. **Completed Applications**: http://localhost:18080 - Review completed jobs  
3. **Application Details**: Click vào application IDs trong web UIs
4. **Airflow Monitoring**: http://localhost:8080 - DAG runs và task details
5. **Celery Monitoring**: http://localhost:5555 - Worker task queues

## Configuration và Scaling

### Environment Variables

Key configuration options trong `.env`:

```bash
# ==========================================
# SPARK CONFIGURATION
# ==========================================
SPARK_VERSION=3.5.0
SPARK_MASTER_PORT=7077
SPARK_MASTER_WEBUI_PORT=1990
SPARK_HISTORY_SERVER_PORT=18080

# Worker configuration
SPARK_WORKER_MEMORY=16G
SPARK_WORKER_CORES=8
SPARK_WORKER_REPLICAS=1

# Performance tuning
SPARK_EXECUTOR_MEMORY=16g
SPARK_EXECUTOR_CORES=8
SPARK_DRIVER_MEMORY=16g

# ==========================================
# JUPYTER CONFIGURATION
# ==========================================
JUPYTER_PORT=8888
JUPYTER_TOKEN=psi
```

### Scaling Workers

```bash
# Scale đến 3 worker nodes
docker-compose up -d --scale spark-worker=3

# Hoặc set trong .env file
SPARK_WORKER_REPLICAS=3
docker-compose up -d
```

### Custom Spark Configuration

Chỉnh sửa `conf/spark-defaults.conf` để tùy chỉnh Spark settings:

```properties
# Memory và performance settings
spark.executor.memory               2g
spark.executor.cores                1
spark.driver.memory                 2g
spark.driver.maxResultSize          1g

# Adaptive query execution
spark.sql.adaptive.enabled          true
spark.sql.adaptive.coalescePartitions.enabled true
spark.sql.shuffle.partitions        200

# Serialization
spark.serializer                    org.apache.spark.serializer.KryoSerializer
spark.sql.execution.arrow.pyspark.enabled true
```

## Data Quality Framework

### Validation Rules theo Layer
- **Bronze Layer**: 
  - Schema compliance với StructType definition
  - Mandatory field validation
  - Data type validation
  - File format validation

- **Silver Layer**: 
  - Deduplication effectiveness testing
  - Join success rate monitoring  
  - Missing data handling validation
  - Data standardization checks

- **Gold Layer**: 
  - Business rule compliance testing
  - Foreign key integrity validation
  - Aggregation accuracy verification
  - KPI calculation validation

### Circuit Breakers và Monitoring
- **Fail pipeline** nếu rejection rate > 10% cho bất kỳ table nào
- **Validation checks** cho mọi layer với detailed reporting
- **JSON reports** cho tất cả DQ checks với pass/fail status
- **XCom integration** để pass metadata giữa các tasks
- **Slack alerts** cho failures và data quality issues

### DQ Implementation Example

```python
# libs/data_quality.py
def validate_bronze_layer(df, config, table_name):
    """Validate bronze layer data quality"""
    total_records = df.count()
    
    # Schema validation
    expected_schema = config['schemas'][table_name]
    schema_valid = validate_schema(df.schema, expected_schema)
    
    # Mandatory field validation  
    null_counts = {}
    for field in config['mandatory_fields'][table_name]:
        null_count = df.filter(col(field).isNull()).count()
        null_counts[field] = null_count
    
    # Calculate rejection rate
    rejected_count = sum(null_counts.values())
    rejection_rate = rejected_count / total_records if total_records > 0 else 0
    
    # Circuit breaker
    if rejection_rate > 0.1:  # 10% threshold
        raise Exception(f"Rejection rate {rejection_rate:.2%} exceeds 10% for table {table_name}")
    
    # Generate DQ report
    dq_report = {
        'table_name': table_name,
        'total_records': total_records,
        'rejected_records': rejected_count,
        'rejection_rate': rejection_rate,
        'schema_valid': schema_valid,
        'null_counts': null_counts,
        'status': 'PASS' if rejection_rate <= 0.1 else 'FAIL'
    }
    
    return dq_report
```

## Troubleshooting và Performance Tuning

### Common Issues

#### Services không khởi động được:
```bash
# Kiểm tra ports có available không
netstat -tulpn | grep -E "(1990|8888|18080|7077|8080|5432|6379)"

# Restart services
docker-compose down && docker-compose up -d

# Kiểm tra Docker resources
docker system df
```

#### Worker nodes không join cluster:
```bash
# Kiểm tra worker logs
docker-compose logs spark-worker

# Verify network connectivity
docker-compose exec spark-worker ping spark-master

# Kiểm tra memory settings
docker-compose exec spark-worker free -h
```

#### Build failures:
```bash
# Clean Docker build cache
docker builder prune -a

# Rebuild without cache
docker-compose build --no-cache

# For Buildx Bake
cd images/custom-notebook
docker buildx bake --no-cache
```

#### Memory issues:
```bash
# Check Docker resources
docker stats

# Clean up unused resources  
docker system prune -a

# Adjust memory limits trong .env
SPARK_WORKER_MEMORY=8G
SPARK_EXECUTOR_MEMORY=4g
```

### Performance Tuning

#### Resource Allocation Strategies

```bash
# High-memory setup (for large datasets)
SPARK_WORKER_MEMORY=32G  
SPARK_WORKER_CORES=8
SPARK_EXECUTOR_MEMORY=8g
SPARK_DRIVER_MEMORY=4g

# Many small tasks setup
SPARK_WORKER_REPLICAS=4
SPARK_EXECUTOR_CORES=2
SPARK_EXECUTOR_MEMORY=2g

# Memory-optimized setup
SPARK_EXECUTOR_MEMORY_FRACTION=0.8
SPARK_DRIVER_MEMORY_FRACTION=0.8
```

#### Spark Configuration Tuning

```properties
# conf/spark-defaults.conf

# Adaptive query execution (Spark 3.0+)
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.skewJoin.enabled=true

# Memory management
spark.executor.memoryFraction=0.8
spark.sql.adaptive.shuffle.targetPostShuffleInputSize=64MB
spark.sql.adaptive.advisoryPartitionSizeInBytes=128MB

# Serialization performance
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.sql.execution.arrow.pyspark.enabled=true

# Network và I/O optimization
spark.network.timeout=120s
spark.sql.broadcastTimeout=36000
spark.storage.level=MEMORY_AND_DISK_SER
```

#### Monitoring và Optimization Best Practices

1. **Resource Monitoring**:
   ```bash
   # Monitor Docker container stats
   docker stats
   
   # Monitor Spark cluster resources
   curl http://localhost:1990/api/v1/applications
   ```

2. **Performance Profiling**:
   - Sử dụng Spark History Server để analyze completed jobs
   - Check stage execution times và identify bottlenecks
   - Monitor shuffle operations và optimize partitioning

3. **Data Optimization**:
   - Partition data theo business keys (e.g., `run_date`)
   - Target 128-512MB Parquet file sizes
   - Use appropriate compression (snappy, gzip, lz4)

## Nguyên tắc phát triển Enterprise

### ✅ REQUIRED PRACTICES
- **Configuration-driven**: Sử dụng YAML configs cho mọi configuration, paths, schemas
- **Error handling**: Implement proper exception handling và Slack alerts
- **Testing**: Viết comprehensive unit tests với `pytest` + `chispa`
- **Idempotency**: Sử dụng `mode('overwrite')` với partitioning cho safe re-runs
- **Vietnamese comments**: Comment code bằng tiếng Việt cho business logic và complex transformations
- **Data quality**: Implement validation checks với circuit breakers
- **Observability**: Add logging, XCom usage, ETL metadata tracking

### ✅ MEDALLION ARCHITECTURE COMPLIANCE
- **Bronze**: Raw data + metadata, schema enforcement, rejection patterns
- **Silver**: Deduplication, cleaning, enrichment với proper joins
- **Gold**: Business metrics, dimensional modeling, SCD Type 2

### ❌ PROHIBITED PRACTICES  
- **Hardcoding**: Never hardcode paths, schemas, business rules trong code
- **Schema inference**: Không sử dụng `inferSchema` - luôn define explicit StructType
- **Skip validation**: Không được skip data quality validation checks
- **Unsafe operations**: Avoid non-idempotent operations hoặc operations không thể re-run
- **Poor error handling**: Không catch exceptions hoặc fail silently
- **Missing documentation**: Code phải có proper Vietnamese comments cho business logic

### 🏗️ DEVELOPMENT METHODOLOGY (15 Steps)

#### Phase 1: Setup & Configuration (Steps 1-5)
1. **Business documentation**: Tạo detailed README với objectives và KPIs
2. **EDA (Exploratory Data Analysis)**: Analyze data với PySpark để hiểu schema và patterns  
3. **Configuration creation**: Build comprehensive `configs/meta_pipeline_config.yaml`
4. **Environment setup**: Configure Docker, Spark, Airflow environments
5. **Data preparation**: Download và organize source data

#### Phase 2: Bronze Layer (Steps 6-7)  
6. **Bronze ingestion**: Load CSV to Parquet với metadata columns và schema validation
7. **Bronze DQ checks**: Implement data quality validation với rejection patterns

#### Phase 3: Silver Layer (Steps 8-9)
8. **Data cleaning**: Deduplication, standardization, logical filtering
9. **Data enrichment**: Joins giữa tables, derived columns, missing data handling

#### Phase 4: Gold Layer (Steps 10-11)
10. **KPI aggregations**: Build business metrics và summary tables
11. **Dimensional modeling**: Create facts và dimensions với SCD Type 2

#### Phase 5: Orchestration & Testing (Steps 12-15)
12. **Airflow DAGs**: Build pipeline orchestration với proper dependencies
13. **Monitoring setup**: Implement XCom usage, metadata tracking, Slack alerts  
14. **Unit testing**: Comprehensive testing với `pytest` + `chispa`
15. **Documentation**: Complete architecture docs, troubleshooting guides
- Ignore performance considerations (file sizes, partitioning)

## Troubleshooting

### Common Issues

**Container startup failures:**
```bash
# Check resources
docker system df
docker system prune

# Restart clean
docker-compose down
docker-compose up -d
```

**Spark connection issues:**
```bash
# Check network connectivity
docker exec -it airflow-worker ping spark-master

# Verify Spark Master URL
echo $SPARK_MASTER_URL
```

**Volume mount issues:**
```bash
# Check permissions
ls -la data/ logs/
chmod -R 755 data/ logs/
```

## Performance Tuning

### Spark Configuration
- **File sizes**: Target 128-512MB Parquet files
- **Partitioning**: Sử dụng `run_date` hoặc business keys
- **Memory**: Tune executor/driver memory theo workload
- **Parallelism**: Adjust based on cluster resources

### Airflow Optimization
- **Concurrency**: Configure based on resources
- **Retries**: Set appropriate retry delays
- **SLA**: Monitor task execution times

## Advanced Features

### Infrastructure as Code (Terraform)

Dự án bao gồm Terraform modules để deploy lên AWS:

```bash
# Navigate to terraform directory
cd terraform/environments/dev

# Initialize Terraform
terraform init

# Plan deployment
terraform plan

# Deploy infrastructure
terraform apply
```

**Terraform modules available:**
- **S3**: Data lake storage với proper lifecycle policies
- **Glue**: Data catalog và ETL jobs
- **Redshift**: Data warehouse cho Gold layer
- **IAM**: Roles và policies cho secure access

### AWS Integration (boto3_deployment)

```bash
# Deploy using boto3 utilities
cd boto3_deployment

# Run deployment
python deploy.py --env dev

# Check deployment status
python resource_inspector.py

# Generate destroy policy
python generate_destroy_policy.py
```

### Monitoring và Alerting

#### Slack Integration
```python
# dags/meta_pipeline_dag.py
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

def task_fail_slack_alert(context):
    slack_msg = f"""
    :red_circle: Task Failed
    *Task*: {context.get('task_instance').task_id}  
    *Dag*: {context.get('task_instance').dag_id}
    *Execution Time*: {context.get('execution_date')}  
    *Log Url*: {context.get('task_instance').log_url}
    """
    
    failed_alert = SlackWebhookOperator(
        task_id='slack_failed',
        http_conn_id='slack_connection',
        message=slack_msg
    )
    return failed_alert.execute(context=context)

# Add to DAG default_args
default_args = {
    'on_failure_callback': task_fail_slack_alert,
    # ... other args
}
```

#### Health Checks
```bash
# Add health check endpoints
curl http://localhost:8080/health  # Airflow health
curl http://localhost:1990/api/v1/applications  # Spark health
curl http://localhost:8888/api  # JupyterLab health
```

## Cleanup và Maintenance

### Regular Cleanup
```bash
# Stop services
docker-compose down

# Clean up logs (optional)
docker-compose down && rm -rf logs/*

# Remove volumes (CAUTION: deletes data!)
docker-compose down -v

# Remove all images
docker-compose down --rmi all

# Clean up Docker system
docker system prune -a
```

### Backup và Recovery
```bash
# Backup important data
mkdir -p backups/$(date +%Y-%m-%d)
cp -r data/ backups/$(date +%Y-%m-%d)/
cp -r configs/ backups/$(date +%Y-%m-%d)/

# Backup Docker volumes
docker run --rm -v spark-recovery:/source -v $(pwd)/backups:/backup alpine tar czf /backup/spark-recovery-$(date +%Y-%m-%d).tar.gz -C /source .
```

## Production Deployment Considerations

### Security Hardening
```bash
# Set secure passwords trong .env
AIRFLOW__WEBSERVER__SECRET_KEY="your-secure-secret-key"
JUPYTER_TOKEN="your-secure-token"

# Enable Spark authentication
SPARK_AUTHENTICATION_ENABLED=yes
SPARK_ENCRYPTION_ENABLED=yes
```

### Performance Optimization
```bash
# Production resource allocation
SPARK_WORKER_MEMORY=64G
SPARK_WORKER_CORES=16
SPARK_WORKER_REPLICAS=4
SPARK_EXECUTOR_MEMORY=16g
SPARK_DRIVER_MEMORY=8g
```

### Monitoring Stack
- **Prometheus**: Metrics collection
- **Grafana**: Visualization dashboards  
- **ELK Stack**: Centralized logging
- **AlertManager**: Alert routing

## References và Documentation

### Official Documentation
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Jupyter Docker Stacks](https://jupyter-docker-stacks.readthedocs.io/en/latest/)
- [Docker Buildx Bake Reference](https://docs.docker.com/engine/reference/commandline/buildx_bake/)

### Custom Images và Extensions
Project này build dựa trên official Jupyter Docker Stacks. Thông tin về:
- **Creating custom Jupyter images**: [Jupyter Docker Stacks - Using Custom Images](https://jupyter-docker-stacks.readthedocs.io/en/latest/using/custom-images.html)
- **Available base images**: [Jupyter Docker Stacks - Selecting an Image](https://jupyter-docker-stacks.readthedocs.io/en/latest/using/selecting.html)
- **Docker Buildx Bake**: [Docker Buildx Bake Documentation](https://docs.docker.com/build/customize/bake/)

### Data Engineering Resources
- [Medallion Architecture Best Practices](https://docs.databricks.com/lakehouse/medallion.html)
- [PySpark Performance Tuning](https://spark.apache.org/docs/latest/tuning.html)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)

### AWS Integration
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [Amazon S3 Best Practices](https://docs.aws.amazon.com/AmazonS3/latest/userguide/best-practices.html)
- [Amazon Redshift Documentation](https://docs.aws.amazon.com/redshift/)

## Contributing

1. **Fork repository** và create feature branch
2. **Follow development guidelines** trong `.github/copilot-instructions.md`
3. **Add comprehensive tests** với pytest + chispa
4. **Test với sample applications** và verify all layers
5. **Submit pull request** với detailed description

### Development Guidelines
- Tuân thủ **Medallion Architecture** patterns
- Implement **configuration-driven** development
- Add **Vietnamese comments** cho business logic
- Follow **15-step methodology** cho complex features
- Ensure **idempotent operations** với proper testing

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

## Support

Để được hỗ trợ và giải quyết vấn đề:
- **Kiểm tra** [troubleshooting section](#troubleshooting-và-performance-tuning)
- **Review logs** sử dụng `docker-compose logs`
- **Tham khảo** [Jupyter Docker Stacks documentation](https://jupyter-docker-stacks.readthedocs.io/)
- **Check** `.github/copilot-instructions.md` cho detailed development guidelines
- **Create issue** trong repository với detailed logs và error messages

---

**Happy Data Pipeline Development! 🚀📊**