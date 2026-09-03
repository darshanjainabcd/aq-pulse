# AQ Pulse
Near-real-time AWS data engineering pipeline for global air-quality analytics using Kinesis, S3, Glue, EMR, Redshift, MWAA,
Step Functions, PySpark and SQL.
## Why this project
Most data-engineering portfolios repeat e-commerce orders, stock prices, or generic clickstream pipelines. AQ Pulse uses
public air-quality data to demonstrate the same production engineering skills in a less common domain while remaining simple
to reproduce.
The source is Open-Meteo Air Quality API. The pipeline polls selected cities, sends records to Amazon Kinesis Data Streams,
stores immutable raw JSON in S3 Bronze, standardizes and deduplicates data with AWS Glue/PySpark into S3 Silver Parquet,
creates daily city/pollutant aggregates with Amazon EMR into S3 Gold, and loads analytics tables into Amazon Redshift. MWAA
schedules the workflow, Step Functions coordinates AWS jobs, CloudWatch provides observability, and IAM/Lake Formation
control access.
## Architecture
```text
Open-Meteo Air Quality API
|
| Python / Lambda / Boto3
v
Amazon Kinesis Data Streams
|
v
Lambda raw landing consumer
|
v
S3 Bronze (JSON, immutable)
|
v
AWS Glue + PySpark
schema validation
deduplication
incremental watermark
standardization
|
v
S3 Silver (Parquet, partitioned)
|
+------&gt; Glue Data Catalog / Athena
|
v
Amazon EMR + PySpark
daily / weekly aggregates
|
v
S3 Gold (Parquet)
|
v
Amazon Redshift
star schema + MERGE/UPSERT
MWAA -&gt; Step Functions -&gt; Glue -&gt; DQ -&gt; EMR -&gt; Redshift
CloudWatch -&gt; logs / metrics / alarms
IAM + Lake Formation -&gt; least-privilege governance
```
## Resume-ready scope
- 25-50 configurable cities.
- Hourly PM2.5, PM10, CO, NO2, SO2, O3 and AQI observations/forecasts.
- Near-real-time ingestion every 10-15 minutes.
- Historical backfill by date range.
- Bronze/Silver/Gold S3 layout.
- Incremental and idempotent processing using source timestamp + city key watermarks.
- SCD Type 2 location dimension in Redshift.
- Data quality, rejected-record handling, retries and CloudWatch monitoring.
## Repository structure
```text
aq-pulse/
├── README.md
├── requirements.txt
├── Dockerfile
├── .gitignore
├── .github/
│ └── workflows/
│ └── ci.yml
├── config/
│ ├── dev.json
│ ├── prod.json
│ └── cities.json
├── src/
│ ├── core/
│ │ ├── __init__.py
│ │ ├── transform.py
│ │ └── quality.py
│ └── ingestion/
│ ├── __init__.py
│ ├── api_client.py
│ ├── kinesis_producer.py
│ └── historical_backfill.py
├── lambda/
│ ├── poll_air_quality/lambda_function.py
│ ├── kinesis_to_s3/lambda_function.py
│ ├── data_quality/lambda_function.py
│ └── redshift_loader/lambda_function.py
├── glue/
│ └── silver_etl.py
├── emr/
│ └── gold_aggregates.py
├── redshift/
│ └── sql/
│ ├── 01_ddl.sql
│ ├── 02_merge_dim_location.sql
│ ├── 03_load_fact.sql
│ └── 04_analytics.sql
├── athena/
│ └── create_external_tables.sql
├── orchestration/
│ ├── mwaa/dags/aq_pipeline_dag.py
│ └── stepfunctions/aq_pipeline.asl.json
├── iam/
│ └── policies/aq_data_engineer_policy.json
├── cloudwatch/
│ └── alarms.json
├── tests/
│ ├── test_transform.py
│ ├── test_quality.py
│ └── test_api_client.py
├── scripts/
│ ├── bootstrap_resources.sh
│ ├── upload_jobs.sh
│ └── run_tests.sh
├── sample_data/
│ └── api_response.json
└── docs/
├── resume_points.md
├── interview_guide.md
└── implementation_steps.md
```
## Data model
### Bronze
Raw API payloads are retained exactly as received for replay and audit.
Path convention:
```text
s3://&lt;bucket&gt;/bronze/air_quality/ingest_date=YYYY-MM-DD/hour=HH/&lt;uuid&gt;.json
```
### Silver
One record per city and event timestamp with standardized column names and data types.
```text
city_id, city, country, latitude, longitude, timezone,
event_ts, pm10, pm2_5, carbon_monoxide, nitrogen_dioxide,
sulphur_dioxide, ozone, european_aqi, source_ingest_ts

Path convention:
```text
s3://&lt;bucket&gt;/silver/air_quality/event_date=YYYY-MM-DD/country=XX/*.parquet
```
### Gold
Daily aggregates by city:
```text
city_id, event_date, avg_pm2_5, max_pm2_5, avg_pm10, max_pm10,
avg_no2, avg_o3, avg_aqi, max_aqi, poor_air_hours, record_count
```
## Redshift star schema
- `dim_location`: SCD Type 2 city/location attributes.
- `dim_date`: date attributes.
- `fact_air_quality_hourly`: hourly pollutant/AQI facts.
- `fact_air_quality_daily`: daily city aggregates.
`dim_location` uses `effective_from`, `effective_to`, and `is_current`. Loads use Redshift `MERGE`/UPSERT patterns for
idempotency and late arriving records.
## Quick implementation
### 1. Prerequisites
- AWS account and AWS CLI configured.
- Python 3.11+ locally.
- IAM permissions for S3, Kinesis, Glue, EMR, Lambda, Redshift, Step Functions, MWAA, CloudWatch and Lake Formation.
- A Redshift Serverless workgroup or provisioned cluster.
- An MWAA environment if you want the managed Airflow portion; otherwise run the state machine manually while developing.
### 2. Configure
Copy `config/dev.json` and replace placeholders for bucket names, region, stream name, Glue job, EMR application and Redshift
identifiers.
### 3. Local smoke test
```bash
python -m unittest discover -s tests -v
```
Fetch one city locally:
```bash
python src/ingestion/api_client.py --city-id pune
```
### 4. Create base AWS resources
Review and run:
```bash
bash scripts/bootstrap_resources.sh
```
The script intentionally keeps IAM role creation explicit so that you can attach the least-privilege policy from
`iam/policies/aq_data_engineer_policy.json` rather than creating an overly broad administrator role.
### 5. Upload jobs
```bash
bash scripts/upload_jobs.sh
```
Create the Glue job using `glue/silver_etl.py`, create an EMR application/job using `emr/gold_aggregates.py`, deploy Lambda
functions, and paste the Step Functions definition after replacing `${...}` placeholders.
### 6. Load Redshift objects
Run the SQL files in order:
```text
01_ddl.sql
02_merge_dim_location.sql
03_load_fact.sql
04_analytics.sql
```
### 7. Orchestrate
Upload `orchestration/mwaa/dags/aq_pipeline_dag.py` to your MWAA DAG S3 path. The DAG invokes the Step Functions workflow on
a schedule and fails the Airflow task if the state machine does not succeed.
## Incremental processing design
Each API record receives a deterministic key:
```text
record_key = city_id + &#39;|&#39; + event_ts
```
Glue removes duplicates on this key and writes Silver partitioned by `event_date` and `country`. Reprocessing the same raw
object therefore does not create duplicate business records. A watermark file can be maintained at:
```text
s3://&lt;bucket&gt;/control/watermarks/air_quality.json
```
For a portfolio implementation, the project can start with a rolling 48-hour read window and deduplication. Once stable,
switch to a persisted watermark.
## Data-quality checks
- Required columns present.
- City ID and timestamp non-null.
- Latitude between -90 and 90.
- Longitude between -180 and 180.
- Pollutant values not negative.
- AQI within expected operational range.
- Duplicate `city_id + event_ts` records rejected.
- Empty Silver/Gold partitions fail the pipeline.
Invalid data is written to:
```text
s3://&lt;bucket&gt;/rejected/air_quality/reason=&lt;reason&gt;/...
```
## Performance choices
- Parquet for Silver/Gold.
- Partition by event date; country is a secondary partition only where it improves pruning.
- Select only required columns before joins/aggregations.
- Broadcast the small city reference dataset in PySpark.
- Avoid unnecessary repartitioning and wide shuffles.
- Use incremental date windows instead of rescanning the entire lake.
- Redshift tables use distribution/sort choices aligned to date and location analytics.
## CloudWatch
Track:
- Lambda errors/throttles.
- Kinesis iterator age and write failures.
- Glue/EMR job failures and durations.
- Step Functions failed executions.
- MWAA task failures.
- Data-quality Lambda failures.
The repository includes starter alarm definitions in `cloudwatch/alarms.json`.
## Security and governance
- IAM least privilege for each runtime role.
- S3 bucket encryption enabled.
- Public access blocked.
- Lake Formation grants for curated Silver/Gold tables.
- Redshift credentials stored outside source code.
- No AWS keys committed to Git.
## CI
GitHub Actions runs Python unit tests and syntax checks on pushes and pull requests. The CI workflow does not deploy AWS
infrastructure automatically, which makes the portfolio repository safe to fork and run without accidental cloud cost.

## Cost-conscious implementation
For a resume project, keep the city list small during development, run Glue/EMR only on demand, stop or delete temporary
compute, and use Redshift Serverless or a short-lived development setup. MWAA is comparatively expensive for a personal
project; implement the DAG code and, if cost matters, run it only long enough to validate screenshots/logs for your
portfolio.
