import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, to_date, trim

print("=== Step 2: Processing Silver Layer with PySpark & Delta Lake ===")

# Check if running in Databricks or local
if "DATABRICKS_RUNTIME_VERSION" in os.environ or "SPARK_REMOTE" in os.environ:
    spark = SparkSession.builder.getOrCreate()
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

# Read Bronze CSV files
bronze_glob_path = "data/bronze/ingest_date=*/*.csv"
print(f"Reading raw data from: {bronze_glob_path}")

raw_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_glob_path)
)

print(f"Loaded {raw_df.count()} raw records into Spark DataFrame.")

# Clean, rename, and enforce schema contracts
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
    .withColumn(
        "production_date",
        to_date(
            concat_ws(
                "-",
                col("prfYear"),
                col("prfMonth"),
                col("prfMonth") * 0 + 1,
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

deduped_df = silver_df.dropDuplicates(
    ["field_name", "production_year", "production_month"]
)

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
deduped_df.show(5, truncate=False)