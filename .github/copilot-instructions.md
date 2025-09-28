# Airflow + PySpark Data Pipeline Project Instructions

## Core Principles (CRITICAL)

**Always follow these fundamental principles:**
- **Medallion Architecture (Bronze → Silver → Gold)**: All solutions must follow this three-layer model
- **Idempotency**: Every data write operation must be safe to re-run using `mode('overwrite')` with partitioning
- **Configuration-Driven**: Never hardcode paths, schemas, or business rules - always use YAML configs
- **Data Quality First**: Implement validation checks and circuit breakers (fail if >10% rejection rate)
- **Observability**: Always include logging, XCom usage, and ETL metadata tracking

## Architecture Overview

This is a **Medallion Architecture** (Bronze → Silver → Gold) data processing pipeline using:
- **Airflow** for orchestration (Docker Compose with CeleryExecutor, Redis, PostgreSQL)
- **PySpark 3.5.0** for data processing (with separate Spark cluster: master, worker, history server)
- **Meta Kaggle datasets** as source data (CSV files in `/opt/airflow/data/meta/raw/`)
- **Parquet** format for processed data storage

## Key Services & Ports

- Airflow Webserver: `http://localhost:8080` (airflow/airflow)
- Spark Master UI: `http://localhost:1990`
- Spark History Server: `http://localhost:18080`
- JupyterLab: `http://localhost:8888`
- Flower (Celery monitoring): `http://localhost:5555` (use `--profile flower`)

## Directory Structure Patterns

```
├── dags/                  # Airflow DAGs
│   └── basic/            # Simple example DAGs
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
└── conf/                 # Spark configuration files
```

## Configuration-Driven Development

**CRITICAL**: Never hardcode paths, schemas, or business rules. Always use:
- YAML config files in `configs/` directory
- Airflow Variables (e.g., `BASE_DATA_DIR`)
- Environment variables from docker-compose

Example pattern for DAGs:
```python
from airflow.models import Variable
base_dir = Variable.get("BASE_DATA_DIR", default_var="/opt/airflow/data")
```

## Data Processing Patterns by Layer

### Bronze Layer (Raw + Metadata)
- **Schema enforcement**: Load data using `StructType` from YAML config (NEVER use `inferSchema`)
- **Metadata addition**: Add `ingest_ts`, `file_name`, `source_system` columns
- **Partitioning**: Save as Parquet partitioned by `source_table`
- **Idempotency**: Always use `mode('overwrite')` for safe re-runs
- **Data Quality**: Implement rejection patterns - save invalid records to `_rejects/` with `reject_reason`
- **Circuit breaker**: Fail pipeline if rejection rate > 10% for any table

### Silver Layer (Cleaned + Enriched)
- **Deduplication**: Use Window functions with `partitionBy` business key, `orderBy ingest_ts`
- **Data cleaning**: Apply logical filtering based on YAML config rules
- **Standardization**: Normalize dates, text fields, country codes
- **Enrichment**: Join tables (e.g., datasets ← users for owner info)
- **Missing data handling**: Use multiple strategies (drop/impute/flag) with documented reasoning
- **Non-matching joins**: Fill with default values (e.g., 'Unknown')

### Gold Layer (Business Metrics + Dimensions)
- **KPI Tables**: Build metrics like `datasets_per_owner`, `competitions_per_year`, `top_tags`
- **Dimensional modeling**: Create dimension tables with Unknown records (surrogate key = -1)
- **SCD Type 2**: Implement slowly changing dimensions with `effective_start_ts`, `effective_end_ts`
- **Fact tables**: Join with dimensions using surrogate keys, handle missing FKs with SK = -1
- **Partitioning**: Partition by `run_date` or relevant business keys
- **Aggregation rules**: Implement business constraints (e.g., total = private + public datasets)

## Spark Integration

**Connection**: Airflow connects to external Spark cluster via `SPARK_MASTER_URL=spark://spark-master:7077`

**Volume mappings** (critical for data access):
```yaml
- ./data:/opt/airflow/data
- ./jobs:/opt/airflow/jobs:ro
- ./libs:/opt/airflow/libs:ro
- ./configs:/opt/airflow/configs:ro
```

## DAG Development Patterns

### Required DAG Structure
- **Daily schedule**: Run at 02:00 Asia/Bangkok timezone
- **Backfill support**: Allow historical data processing
- **SLA requirements**: Silver → Gold processing < 10 minutes
- **Dependencies**: Bronze → Silver → Gold with proper task ordering
- **Retries**: Configure `retries >= 1` with appropriate retry delays

### Common Imports
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta
```

### Configuration Loading Pattern
```python
# Luôn load config từ YAML file, không hardcode
import yaml
from airflow.models import Variable

base_dir = Variable.get("BASE_DATA_DIR", default_var="/opt/airflow/data")
with open('/opt/airflow/configs/meta_pipeline_config.yaml', 'r') as f:
    config = yaml.safe_load(f)
```

### Error Handling & Monitoring
- **Slack integration**: Use `on_failure_callback` for failure notifications
- **Path validation**: Implement `resolve_path()` helper for security
- **File validation**: Always check file existence before processing
- **XCom usage**: Pass metadata between tasks using `ti.xcom_pull(task_ids='task_name')`
- **ETL metadata**: Track run information in `etl_metadata` table

## Testing & Quality Assurance

### Unit Testing Requirements
Use `pytest` + `chispa` for comprehensive PySpark testing:
- **Schema tests**: `assert_schema_equality` to validate data structure
- **Transformation tests**: `assert_df_equality` for data transformation logic
- **Aggregation tests**: Verify business logic correctness and constraints
- **Data quality tests**: Validate rejection rates and circuit breaker logic
- **Integration tests**: Test full Bronze → Silver → Gold pipeline flows

### Data Quality Framework
- **Bronze validation**: Schema compliance, data type validation, mandatory field checks
- **Silver validation**: Deduplication effectiveness, missing data handling, join success rates
- **Gold validation**: Business rule compliance, FK integrity, aggregation accuracy
- **Reporting**: Generate JSON reports for all DQ checks with pass/fail status
- **Circuit breakers**: Stop pipeline execution if critical quality thresholds are breached

## Development Workflow

1. **Start services**: `docker-compose up -d`
2. **Check logs**: `docker-compose logs -f airflow-scheduler`
3. **Access Airflow**: Navigate to `http://localhost:8080`
4. **Monitor Spark**: Use History Server UI for job performance
5. **Debug**: Use JupyterLab for interactive development

## Docker Commands

```bash
# Build custom Airflow image
docker-compose build

# Start all services
docker-compose up -d

# Check service health
docker-compose ps

# View logs
docker-compose logs -f [service-name]

# Clean restart
docker-compose down && docker-compose up -d
```

## Data Quality Framework

- Implement DQ checks that write results to JSON files
- Use rejection patterns: save invalid records to `_rejects/` directories
- Implement circuit breakers: fail pipeline if rejection rate > 10%
- Build `etl_metadata` table to track run information

## Performance Considerations

- **Parquet files**: Target 128-512MB file sizes
- **Partitioning**: Use `run_date` or business keys
- **Avoid small files**: Monitor partition sizes
- **Spark configuration**: Tuned via `conf/spark-defaults.conf`

## Language & Communication Guidelines

### Vietnamese Response Requirements
- **Always respond in Vietnamese** when providing explanations, guidance, or troubleshooting help
- **Code comments must be in Vietnamese** to explain business logic and complex transformations
- Use Vietnamese for DAG descriptions, task explanations, and error messages where possible
- English is acceptable only for technical keywords, library names, and standard configuration keys

### Code Comment Patterns
```python
# Đọc dữ liệu từ Bronze layer và thực hiện deduplication
datasets_df = spark.read.parquet(bronze_path)

# Sử dụng Window function để loại bỏ bản ghi trùng lặp
# Giữ lại bản ghi mới nhất dựa trên ingest_ts
window_spec = Window.partitionBy("dataset_id").orderBy(desc("ingest_ts"))
deduplicated_df = datasets_df.withColumn("row_num", row_number().over(window_spec)) \
                            .filter(col("row_num") == 1) \
                            .drop("row_num")
```

### Response Style
- **Incremental guidance**: Break complex tasks into smaller steps with Vietnamese explanations
- **Contextual advice**: Reference specific files and patterns from this codebase
- **Practical examples**: Provide code snippets with Vietnamese comments explaining the "why"
- **Error troubleshooting**: Explain common issues and solutions in Vietnamese
- **Stage-by-stage approach**: Follow the 15-step methodology from setup to documentation
- **Config-first reminders**: Always emphasize loading from YAML configs, never hardcoding

## Development Methodology (15 Steps)

### Phase 1: Setup & Configuration (Steps 1-5)
1. **Business documentation**: Create `META_README.md` with objectives and KPIs
2. **EDA (Exploratory Data Analysis)**: Analyze data with PySpark to understand schema and patterns
3. **Configuration creation**: Build `configs/meta_pipeline_config.yaml` with paths, schema, DQ rules

### Phase 2: Bronze Layer (Steps 6-7)
4. **Bronze ingestion**: Load CSV to Parquet with metadata columns and schema validation
5. **Bronze DQ checks**: Implement data quality validation with rejection patterns

### Phase 3: Silver Layer (Steps 8-9)
6. **Data cleaning**: Deduplication, standardization, logical filtering
7. **Data enrichment**: Joins between tables, derived columns, missing data handling

### Phase 4: Gold Layer (Steps 10-11)
8. **KPI aggregations**: Build business metrics and summary tables
9. **Dimensional modeling**: Create facts and dimensions with SCD Type 2

### Phase 5: Orchestration & Testing (Steps 12-15)
10. **Airflow DAGs**: Build pipeline orchestration with proper dependencies
11. **Monitoring setup**: Implement XCom usage, metadata tracking, Slack alerts
12. **Unit testing**: Comprehensive testing with `pytest` + `chispa`
13. **Documentation**: Complete architecture docs, troubleshooting guides

## Critical Success Factors

- **Never skip configuration**: Every path, schema, rule must come from YAML
- **Always validate**: Implement DQ checks at every layer with proper reporting
- **Idempotency first**: Every operation must be safe to re-run
- **Vietnamese explanations**: All guidance and comments must be in Vietnamese
- **Enterprise patterns**: Follow medallion architecture and data engineering best practices

This project follows enterprise data engineering best practices with emphasis on maintainability, observability, and data quality.