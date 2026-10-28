import os
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, to_date, trim

print("=== Step 3: Incremental Delta Lake Upsert (MERGE INTO) ===")

is_databricks = (
    "DATABRICKS_RUNTIME_VERSION" in os.environ or "SPARK_REMOTE" in os.environ
)

if is_databricks:
    spark = SparkSession.builder.getOrCreate()
else:
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

base_dir = os.path.dirname(os.path.abspath(__file__))
bronze_glob_path = (
    f"file:{os.path.join(base_dir, 'data/bronze/ingest_date=*/*.csv')}"
)

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

if is_databricks:
    target_table_name = "ncs_production_silver"
    target_table = DeltaTable.forName(spark, target_table_name)
else:
    target_delta_path = "data/silver/ncs_production_silver"
    target_table = DeltaTable.forPath(spark, target_delta_path)

print("Executing ACID MERGE INTO matching on composite key...")

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

print("SUCCESS: Delta MERGE completed.")
history_df = target_table.history().select(
    "version", "timestamp", "operation", "operationParameters"
)
history_df.show(3, truncate=False)