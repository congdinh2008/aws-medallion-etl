# Airflow + PySpark Data Pipeline Project

## Tổng quan

Đây là một data pipeline theo **Medallion Architecture** (Bronze → Silver → Gold) để xử lý dữ liệu Meta Kaggle sử dụng Apache Airflow cho orchestration và PySpark cho data processing.

### Kiến trúc chính

- **Airflow**: Orchestration với CeleryExecutor, Redis, PostgreSQL
- **Spark Cluster**: Master, Worker, History Server (PySpark 3.5.0)
- **Storage**: Parquet format cho hiệu suất tối ưu
- **Data Source**: Meta Kaggle datasets (CSV)
- **Development**: JupyterLab cho interactive development

## Cấu trúc thư mục

```
├── dags/                  # Airflow DAGs
│   └── basic/            # Example DAGs
├── jobs/                 # PySpark job scripts
│   └── meta/            # Meta Kaggle processing jobs
├── libs/                 # Reusable Python utilities
├── configs/              # YAML configuration files
├── data/                 # Data storage
│   └── meta/
│       ├── raw/         # CSV source files
│       ├── bronze/      # Raw + metadata (Parquet)
│       ├── silver/      # Cleaned data (Parquet)
│       └── gold/        # Business metrics (Parquet)
├── conf/                 # Spark configuration
├── notebooks/            # Jupyter notebooks
├── tests/                # Unit tests
└── requirements/         # Documentation & requirements
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

### Phần mềm
- Docker & Docker Compose
- Python 3.8+
- Java 11 (cho PySpark)

### Tài nguyên
- RAM: Tối thiểu 8GB (khuyến nghị 16GB+)
- CPU: Tối thiểu 4 cores
- Disk: Tối thiểu 20GB free space

## Cài đặt và chạy

### 1. Clone repository

```bash
git clone <repository-url>
cd airflow-compose
```

### 2. Chuẩn bị môi trường

```bash
# Copy environment file
cp .env.example .env

# Tạo thư mục cần thiết
mkdir -p data/meta/{raw,bronze,silver,gold}
mkdir -p configs jobs/meta libs tests
```

### 3. Chuẩn bị dữ liệu Meta Kaggle

Tải xuống các file CSV từ Kaggle Meta dataset và đặt vào `data/meta/raw/`:
- `Datasets.csv`
- `Competitions.csv` 
- `Users.csv`
- `Tags.csv`
- `Kernels.csv`

### 4. Build và khởi động services

```bash
# Build custom Airflow image
docker-compose build

# Khởi động tất cả services
docker-compose up -d

# Kiểm tra trạng thái
docker-compose ps
```

### 5. Truy cập các services

- **Airflow UI**: http://localhost:8080 (airflow/airflow)
- **Spark Master UI**: http://localhost:1990
- **Spark History Server**: http://localhost:18080
- **JupyterLab**: http://localhost:8888
- **Flower (Celery Monitor)**: http://localhost:5555 (cần `--profile flower`)

## Phát triển

### DAG Development

```python
# Luôn sử dụng configuration-driven approach
from airflow.models import Variable
base_dir = Variable.get("BASE_DATA_DIR", default_var="/opt/airflow/data")

# Load config từ YAML
import yaml
with open('/opt/airflow/configs/meta_pipeline_config.yaml', 'r') as f:
    config = yaml.safe_load(f)
```

### Testing

```bash
# Chạy unit tests
docker exec -it <airflow-worker-container> pytest tests/

# Test PySpark jobs
python -m pytest tests/ -v --tb=short
```

### Debugging

```bash
# Xem logs
docker-compose logs -f airflow-scheduler
docker-compose logs -f spark-master

# Access container
docker exec -it <container-name> bash
```

## Data Quality Framework

### Validation Rules
- **Bronze**: Schema compliance, mandatory fields
- **Silver**: Deduplication effectiveness, join success rates  
- **Gold**: Business rule compliance, FK integrity

### Circuit Breakers
- Fail pipeline nếu rejection rate > 10%
- Validation checks cho mọi layer
- JSON reports cho tất cả DQ checks

## Nguyên tắc phát triển

### ✅ DO
- Sử dụng YAML configs cho mọi configuration
- Implement proper error handling và Slack alerts
- Viết unit tests với `pytest` + `chispa`
- Sử dụng `mode('overwrite')` cho idempotency
- Comment code bằng tiếng Việt cho business logic

### ❌ DON'T
- Hardcode paths, schemas, business rules
- Sử dụng `inferSchema` (luôn define StructType)
- Skip data quality validation
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

## Đóng góp

1. Fork repository
2. Tạo feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Tạo Pull Request

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

## Liên hệ

- **Project Maintainer**: [Tên của bạn]
- **Email**: [Email của bạn]
- **Documentation**: See `.github/copilot-instructions.md` for detailed development guidelines