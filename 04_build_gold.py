import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, min, round, sum, when

print("=== Step 4: Building Gold Dimensional Marts (Star Schema) ===")

is_databricks = (
    "DATABRICKS_RUNTIME_VERSION" in os.environ or "SPARK_REMOTE" in os.environ
)

if is_databricks:
    spark = SparkSession.builder.getOrCreate()
    silver_df = spark.table("ncs_production_silver")
else:
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName("NCSGoldProcessing")
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

    base_dir = (
        os.path.dirname(os.path.abspath(__file__))
        if "__file__" in globals()
        else os.getcwd()
    )
    silver_df = spark.read.format("delta").load(
        os.path.join(base_dir, "data/silver/ncs_production_silver")
    )

print(f"Loaded {silver_df.count()} clean Silver records.")

# Dimension Table: dim_field
print("Generating Gold Dimension: dim_field...")
dim_field = (
    silver_df.groupBy("field_name", "operator")
    .agg(min("production_year").alias("first_production_year"))
    .orderBy("field_name")
)

# Fact Table: fct_monthly_production
print("Generating Gold Fact: fct_monthly_production...")
fct_production = (
    silver_df.withColumn(
        "water_cut_pct",
        when(
            (col("oil_net_mill_sm3") + col("water_net_mill_sm3")) > 0,
            round(
                (
                    col("water_net_mill_sm3")
                    / (col("oil_net_mill_sm3") + col("water_net_mill_sm3"))
                )
                * 100,
                2,
            ),
        ).otherwise(0.0),
    )
    .withColumn(
        "oil_million_bbl", round(col("oil_net_mill_sm3") * 6.29, 2)
    )
    .select(
        "field_name",
        "production_date",
        "production_year",
        "production_month",
        "oil_net_mill_sm3",
        "oil_million_bbl",
        "gas_net_bill_sm3",
        "water_net_mill_sm3",
        "water_cut_pct",
    )
)

if is_databricks:
    dim_field.write.format("delta").mode("overwrite").saveAsTable("dim_field")
    (
        fct_production.write.format("delta")
        .mode("overwrite")
        .partitionBy("production_year")
        .saveAsTable("fct_monthly_production")
    )
else:
    base_dir = (
        os.path.dirname(os.path.abspath(__file__))
        if "__file__" in globals()
        else os.getcwd()
    )
    gold_dir = os.path.join(base_dir, "data/gold")
    os.makedirs(gold_dir, exist_ok=True)
    dim_field.write.format("delta").mode("overwrite").save(
        os.path.join(gold_dir, "dim_field")
    )
    (
        fct_production.write.format("delta")
        .mode("overwrite")
        .partitionBy("production_year")
        .save(os.path.join(gold_dir, "fct_monthly_production"))
    )

print("SUCCESS: Gold dimension and fact tables created.")

print("\n--- Executive Summary: Total Oil Produced by Operator (Gold) ---")
operator_summary = (
    fct_production.join(dim_field, on="field_name")
    .groupBy("operator")
    .agg(round(sum("oil_million_bbl"), 2).alias("total_oil_million_bbl"))
    .orderBy(desc("total_oil_million_bbl"))
)
operator_summary.show(truncate=False)