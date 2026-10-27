import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, min, round, sum, when

print("=== Step 4: Building Gold Dimensional Marts (Star Schema) ===")

try:
    spark = SparkSession.builder.getOrCreate()
except Exception:
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

# 2. Read from curated Silver Delta Lake
silver_path = "data/silver/ncs_production_silver"
silver_df = spark.read.format("delta").load(silver_path)

print(f"Loaded {silver_df.count()} clean Silver records.")

# ---------------------------------------------------------
# MART 1: Dimension Table - dim_field
# ---------------------------------------------------------
print("Generating Gold Dimension: dim_field...")

dim_field = (
    silver_df.groupBy("field_name", "operator")
    .agg(min("production_year").alias("first_production_year"))
    .orderBy("field_name")
)

gold_dim_path = "data/gold/dim_field"
os.makedirs("data/gold", exist_ok=True)

dim_field.write.format("delta").mode("overwrite").save(gold_dim_path)
print(f" -> Saved dim_field to: {gold_dim_path}")

# ---------------------------------------------------------
# MART 2: Fact Table - fct_monthly_production
# Deriving business KPIs: Water Cut % and BOE (Barrels of Oil Equivalent)
# ---------------------------------------------------------
print("Generating Gold Fact: fct_monthly_production...")

fct_production = (
    silver_df.withColumn(
        # Water Cut % = Water / (Oil + Water)
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
        # Approximate conversion factor: 1 Sm3 oil ~= 6.29 barrels
        "oil_million_bbl",
        round(col("oil_net_mill_sm3") * 6.29, 2),
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

gold_fct_path = "data/gold/fct_monthly_production"
(
    fct_production.write.format("delta")
    .mode("overwrite")
    .partitionBy("production_year")
    .save(gold_fct_path)
)
print(f" -> Saved fct_monthly_production to: {gold_fct_path}")

# ---------------------------------------------------------
# Business Validation Query: Top 5 Asset Operators by Volume
# ---------------------------------------------------------
print("\n--- Executive Summary: Total Oil Produced by Operator (Gold) ---")
operator_summary = (
    fct_production.join(dim_field, on="field_name")
    .groupBy("operator")
    .agg(round(sum("oil_million_bbl"), 2).alias("total_oil_million_bbl"))
    .orderBy(desc("total_oil_million_bbl"))
)

operator_summary.show(truncate=False)

spark.stop()