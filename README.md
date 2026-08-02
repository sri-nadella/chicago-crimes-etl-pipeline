# Chicago Crimes ETL Pipeline — AWS Glue + S3

End-to-end batch ETL pipeline that ingests, cleans, and transforms Chicago crime data using AWS Glue (PySpark), S3, and outputs analytics-ready Parquet files. Built as a portfolio project demonstrating real cloud data engineering on AWS.

---

## Architecture

```
Chicago Data Portal (CSV)
          ↓
    S3 Raw Zone                  ← Landing zone for raw CSV files
    (chicago-crimes-etl-pipeline/raw/)
          ↓
    AWS Glue ETL Job             ← PySpark transformation script
    (chicago-crimes-etl-job)
          ↓
    S3 Processed Zone            ← Analytics-ready Parquet output
    (chicago-crimes-etl-pipeline/processed/crimes/)
```

---

## Dataset

- **Source:** City of Chicago Open Data Portal
- **Dataset:** Crimes — 2023
- **Size:** ~260,000 records
- **Format:** CSV (raw) → Parquet (processed, Snappy compressed)
- **Fields:** Crime ID, Case Number, Date, Block, Crime Type, Description, Location, Arrest, District, Ward, Community Area, Coordinates

---

## What the Pipeline Does

### 1. Ingestion
Reads raw CSV from S3 landing zone using PySpark with multi-line and escape character handling.

### 2. Column Selection & Renaming
Selects 19 relevant columns and renames them to clean snake_case format:
- `Primary Type` → `crime_type`
- `Location Description` → `location_description`
- `Case Number` → `case_number`
- And so on

### 3. Transformations
- Parses `crime_date` from raw string format (`MM/dd/yyyy hh:mm:ss a`)
- Extracts time features: `crime_month`, `crime_hour`, `day_of_week`, `day_name`
- Standardizes text fields to uppercase and trims whitespace
- Flags arrest and domestic fields consistently

### 4. Data Quality Filters
Removes records where:
- `crime_id` is null
- `crime_date` cannot be parsed
- `crime_type` is missing

### 5. Output
Writes clean data to S3 as **Snappy-compressed Parquet** — 9.3 MB compressed from the original CSV, optimized for analytics queries.

---

## AWS Services Used

| Service | Role |
|---|---|
| Amazon S3 | Raw landing zone + processed output storage |
| AWS Glue (Spark) | PySpark ETL transformation job |
| IAM | Role-based access control for Glue → S3 |

---

## Pipeline Run Results

| Run | Status | Duration | Workers | Output |
|---|---|---|---|---|
| Run 1 | ❌ Failed | 49s | 2 DPUs G.1X | File path error (fixed) |
| Run 2 | ✅ Succeeded | 1m 29s | 2 DPUs G.1X | 9.3 MB Parquet in S3 |

---

## Project Structure

```
chicago-crimes-etl/
├── scripts/
│   └── glue_etl_chicago.py     # AWS Glue PySpark ETL script
├── sql/
│   └── create_tables.sql       # Redshift table DDL (for warehouse load)
├── docs/
│   └── architecture.png        # Pipeline architecture diagram
└── README.md
```

---

## How to Run This Pipeline

### Prerequisites
- AWS account with access to S3 and Glue
- IAM role with `AWSGlueServiceRole` and `AmazonS3FullAccess` policies

### Steps
1. Download Chicago Crimes 2023 CSV from the Chicago Data Portal
2. Upload to `s3://your-bucket/raw/`
3. Upload `glue_etl_chicago.py` to `s3://your-bucket/scripts/`
4. Create a Glue job pointing to the script
5. Set IAM role, Glue 4.0, G.1X worker type, 2 workers
6. Run the job — output Parquet appears in `s3://your-bucket/processed/crimes/`

---

## Key Technical Decisions

**Why Parquet over CSV?**
Parquet is columnar — queries that filter or aggregate on specific columns (e.g. `crime_type`, `district`) scan only those columns, not the entire file. This makes downstream analytics 10–50x faster and reduces storage by ~70% through Snappy compression.

**Why AWS Glue over Lambda?**
This dataset has 260,000 rows. Lambda has a 15-minute timeout and limited memory. Glue Spark scales horizontally — adding more DPUs handles 10x the data with no code changes.

**Why extract time features at ETL time?**
Pre-computing `crime_hour`, `day_of_week`, and `day_name` at pipeline time means every downstream query gets these for free — no repeated date parsing in analytics.

---

## Sample Analytics Queries

Once loaded into Redshift or queried via Athena, you can answer questions like:

```sql
-- Top 10 crime types in 2023
SELECT crime_type, COUNT(*) as total
FROM crimes
GROUP BY crime_type
ORDER BY total DESC
LIMIT 10;

-- Arrest rate by district
SELECT district,
       COUNT(*) as total_crimes,
       SUM(CASE WHEN arrest = 'TRUE' THEN 1 ELSE 0 END) as arrests,
       ROUND(100.0 * SUM(CASE WHEN arrest = 'TRUE' THEN 1 ELSE 0 END) / COUNT(*), 2) as arrest_rate_pct
FROM crimes
GROUP BY district
ORDER BY arrest_rate_pct DESC;

-- Crime volume by hour of day
SELECT crime_hour, COUNT(*) as total
FROM crimes
GROUP BY crime_hour
ORDER BY crime_hour;
```

---

## Tech Stack

```
Cloud:      AWS (S3, Glue)
Processing: PySpark (AWS Glue 4.0)
Storage:    Parquet (Snappy compression)
Language:   Python 3
IAM:        AWSGlueServiceRole + AmazonS3FullAccess
```
