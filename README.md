# University Academic Performance Analytics Platform

A production-grade data engineering platform for university academic analytics, featuring:

- **Data Warehouse**: Star Schema on Snowflake (Students, Courses, Departments, Time dimensions)
- **ETL/ELT Pipelines**: Python-based with Pandas + SQLAlchemy
- **Apache Spark**: Batch aggregations + Kafka streaming
- **Orchestration**: Apache Airflow DAG
- **Security**: RBAC, Dynamic Data Masking, Row-Level Security
- **API**: FastAPI with JWT auth
- **Dashboard**: Plotly Dash with interactive analytics

## Quick Start

```bash
# Clone and setup environment
cp .env.example .env        # Fill in your credentials
pip install -r requirements.txt

# Spin up local services (Postgres, Kafka, Airflow)
docker-compose up -d

# Initialize warehouse schema
python -m warehouse.ddl.init_schema

# Run ETL
python etl/main_etl.py

# Run Spark batch jobs
spark-submit spark_jobs/batch/semester_aggregation.py

# Start API
uvicorn api.main:app --reload

# Start Dashboard
python dashboard/app.py
```

## Architecture

```
SIS / LMS / Attendance Systems
         │
    [Extract Layer]  ← extract_sis.py, extract_lms.py, extract_attendance.py
         │
    [Raw Data]       ← data/raw/  (CSV / JSON)
         │
    [Transform]      ← clean_students.py, calculate_gpa.py, validation_rules.py
         │
    [Staging]        ← PostgreSQL / Snowflake STAGING schema
         │
    [Load]           ← merge_fact_tables.py (MERGE INTO star schema)
         │
    [Snowflake DW]   ← PROD schema: DIM_* + FACT_* tables
         │
   ┌─────┼──────────┐
[Spark] [API]  [Dashboard]
Batch+Stream  FastAPI  Plotly Dash
```

## Project Structure

| Folder | Purpose |
|--------|---------|
| `config/` | Settings, Snowflake config, logging |
| `etl/` | Extract → Transform → Load pipeline |
| `spark_jobs/` | Batch & streaming Spark jobs |
| `airflow/` | Airflow DAG for orchestration |
| `warehouse/` | DDL, transformations, analytics SQL, security |
| `api/` | FastAPI backend |
| `dashboard/` | Plotly Dash frontend |
| `tests/` | Unit + integration tests |
| `docs/` | Architecture diagrams, design doc |

## Data Model (Star Schema)

**Fact Tables**
- `FACT_ACADEMIC_PERFORMANCE` — enrollment, grade, GPA per student/course/semester
- `FACT_ATTENDANCE` — daily attendance per student/course

**Dimension Tables**
- `DIM_STUDENT` — student demographics + SCD Type 2
- `DIM_COURSE` — course catalog
- `DIM_DEPARTMENT` — department hierarchy
- `DIM_INSTRUCTOR` — faculty
- `DIM_DATE` — date dimension (day/week/month/semester)

## Environment Variables

| Variable | Description |
|----------|-------------|
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier |
| `SNOWFLAKE_USER` | Username |
| `SNOWFLAKE_PASSWORD` | Password |
| `SNOWFLAKE_DATABASE` | Database name |
| `SNOWFLAKE_WAREHOUSE` | Compute warehouse |
| `POSTGRES_URI` | Local Postgres for staging |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address |
| `JWT_SECRET_KEY` | API JWT signing secret |