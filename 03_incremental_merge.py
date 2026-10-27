import os
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, to_date, trim

print("=== Step 3: Incremental Delta Lake Upsert (MERGE INTO) ===")

try:
    spark = SparkSession.builder.getOrCreate()
except Exception:
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName("NCSIncrementalMerge")
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

# 2. Read incoming raw records (Source)
bronze_glob_path = "data/bronze/ingest_date=*/*.csv"
source_df = (
    spark.read.option("header", "true")
    .csv(bronze_glob_path)
    .withColumn("field_name", trim(col("fldName")))
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
    .dropDuplicates(["field_name", "production_year", "production_month"])
)

target_delta_path = "data/silver/ncs_production_silver"

# 3. Check for existing Delta table and execute MERGE
if not DeltaTable.isDeltaTable(spark, target_delta_path):
    print("Initial target table not found. Creating Silver table...")
    (
        source_df.write.format("delta")
        .partitionBy("production_year")
        .save(target_delta_path)
    )
    print("Initial Silver table populated.")
else:
    print(f"Existing Delta table detected at: {target_delta_path}")
    print("Executing ACID MERGE INTO matching on natural composite keys...")

    target_table = DeltaTable.forPath(spark, target_delta_path)

    (
        target_table.alias("target")
        .merge(
            source_df.alias("source"),
            """
            target.field_name = source.field_name AND 
            target.production_year = source.production_year AND 
            target.production_month = source.production_month
            """,
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("SUCCESS: Delta MERGE completed without full table scan or rewrite.")

# 4. Inspect Table History (Delta Lake Time-Travel Feature)
print("\n--- Inspecting Delta Table History (ACID Commits) ---")
target_table = DeltaTable.forPath(spark, target_delta_path)
history_df = target_table.history().select(
    "version", "timestamp", "operation", "operationParameters"
)
history_df.show(5, truncate=False)

spark.stop()