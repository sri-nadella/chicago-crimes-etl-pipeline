import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ── Step 1: Read raw CSV from S3 ──────────────────────────────
print("Reading raw data from S3...")
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", '"') \
    .load("s3://chicago-crimes-etl-pipeline/raw/Crimes_-_2023_20260801.csv")

print(f"Raw record count: {df.count()}")

# ── Step 2: Select and rename columns ────────────────────────
df = df.select(
    F.col("ID").alias("crime_id"),
    F.col("Case Number").alias("case_number"),
    F.col("Date").alias("crime_date_raw"),
    F.col("Block").alias("block"),
    F.col("Primary Type").alias("crime_type"),
    F.col("Description").alias("description"),
    F.col("Location Description").alias("location_description"),
    F.col("Arrest").alias("arrest"),
    F.col("Domestic").alias("domestic"),
    F.col("District").alias("district"),
    F.col("Ward").alias("ward"),
    F.col("Community Area").alias("community_area"),
    F.col("Year").alias("year"),
    F.col("Latitude").alias("latitude"),
    F.col("Longitude").alias("longitude")
)

# ── Step 3: Clean and transform ───────────────────────────────
print("Cleaning data...")

# Parse date
df = df.withColumn(
    "crime_date",
    F.to_timestamp(F.col("crime_date_raw"), "MM/dd/yyyy hh:mm:ss a")
)

# Extract time features
df = df.withColumn("crime_month", F.month("crime_date")) \
       .withColumn("crime_hour", F.hour("crime_date")) \
       .withColumn("day_of_week", F.dayofweek("crime_date")) \
       .withColumn("day_name", F.date_format("crime_date", "EEEE"))

# Standardize text
df = df.withColumn("crime_type", F.upper(F.trim(F.col("crime_type")))) \
       .withColumn("arrest", F.upper(F.trim(F.col("arrest")))) \
       .withColumn("domestic", F.upper(F.trim(F.col("domestic"))))

# ── Step 4: Data quality filters ──────────────────────────────
print("Applying quality filters...")
df_clean = df.filter(
    F.col("crime_id").isNotNull() &
    F.col("crime_date").isNotNull() &
    F.col("crime_type").isNotNull()
)

# Select final columns
df_final = df_clean.select(
    "crime_id", "case_number", "crime_date",
    "crime_month", "crime_hour", "day_of_week", "day_name",
    "block", "crime_type", "description",
    "location_description", "arrest", "domestic",
    "district", "ward", "community_area",
    "year", "latitude", "longitude"
)

print(f"Clean record count: {df_final.count()}")

# ── Step 5: Write to S3 processed zone ────────────────────────
print("Writing to S3 processed zone...")
df_final.write.mode("overwrite") \
    .parquet("s3://chicago-crimes-etl-pipeline/processed/crimes/")

print("Pipeline complete!")
job.commit()
