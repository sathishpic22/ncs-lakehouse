import os
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, to_date, trim

print("=== Step 2: Processing Silver Layer with PySpark & Delta Lake ===")

is_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ

if is_databricks:
    spark = SparkSession.builder.appName("NCSSilverProcessing").getOrCreate()
else:
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName("NCSSilverProcessing")
        .master("local[*]")
        .config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Read Bronze CSV files (reads across any ingest_date partitions)
bronze_glob_path = "data/bronze/ingest_date=*/*.csv"
print(f"Reading raw data from: {bronze_glob_path}")

raw_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_glob_path)
)

record_count = raw_df.count()
print(f"Loaded {record_count} raw records into Spark DataFrame.")

# 3. Clean, rename, and enforce schema contracts
silver_df = (
    raw_df.withColumn("field_name", trim(col("fldName")))
    .withColumn("operator", trim(col("fldOperator")))
    .withColumn("production_year", col("prfYear").cast("int"))
    .withColumn("production_month", col("prfMonth").cast("int"))
    .withColumn("oil_net_mill_sm3", col("prfPrdOilNetMillSm3").cast("double"))
    .withColumn("gas_net_bill_sm3", col("prfPrdGasNetBillSm3").cast("double"))
    .withColumn(
        "water_net_mill_sm3", col("prfPrdWaterNetMillSm3").cast("double")
    )
    # Construct an actual ISO Date column (e.g., 2026-03-01) for time-series analysis
    .withColumn(
        "production_date",
        to_date(
            concat_ws(
                "-",
                col("prfYear"),
                col("prfMonth"),
                col("prfMonth") * 0 + 1,  # Day 1 of each month
            ),
            "yyyy-M-d",
        ),
    )
    .select(
        "field_name",
        "operator",
        "production_date",
        "production_year",
        "production_month",
        "oil_net_mill_sm3",
        "gas_net_bill_sm3",
        "water_net_mill_sm3",
    )
)

# 4. Enforce uniqueness: drop duplicate events across primary composite keys
deduped_df = silver_df.dropDuplicates(
    ["field_name", "production_year", "production_month"]
)

# 5. Write to Silver Delta Lake Table partitioned by production_year
silver_delta_path = "data/silver/ncs_production_silver"
os.makedirs("data/silver", exist_ok=True)

print(f"Writing curated data to Delta Lake at: {silver_delta_path}...")

(
    deduped_df.write.format("delta")
    .mode("overwrite")
    .partitionBy("production_year")
    .save(silver_delta_path)
)

print("SUCCESS! Silver Delta Lake table created.")

# 6. Verify table contents and schema via Spark
print("\n--- Silver Delta Table Schema ---")
deduped_df.printSchema()

print("\n--- Preview of Processed Records ---")
deduped_df.show(5, truncate=False)

spark.stop()