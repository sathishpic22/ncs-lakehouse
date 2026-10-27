import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip


@pytest.fixture(scope="session")
def spark():
    """Initializes Spark session once for the test suite."""
    builder = (
        SparkSession.builder.appName("LakehouseQualityTests")
        .master("local[2]")
        .config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark_session = configure_spark_with_delta_pip(builder).getOrCreate()
    spark_session.sparkContext.setLogLevel("ERROR")
    yield spark_session
    spark_session.stop()


def test_fact_no_negative_production_volumes(spark):
    """Rule 1: Net production metrics must never be negative."""
    fct_df = spark.read.format("delta").load("data/gold/fct_monthly_production")

    invalid_rows = fct_df.filter(
        (col("oil_net_mill_sm3") < 0)
        | (col("gas_net_bill_sm3") < 0)
        | (col("water_net_mill_sm3") < 0)
    ).count()

    assert (
        invalid_rows == 0
    ), f"Found {invalid_rows} records with negative production!"


def test_water_cut_percentage_range(spark):
    """Rule 2: Water cut percentage must strictly fall between 0.0 and 100.0."""
    fct_df = spark.read.format("delta").load("data/gold/fct_monthly_production")

    out_of_bounds = fct_df.filter(
        (col("water_cut_pct") < 0.0) | (col("water_cut_pct") > 100.0)
    ).count()

    assert (
        out_of_bounds == 0
    ), f"Found {out_of_bounds} records with invalid water cut %!"


def test_referential_integrity_dimension_coverage(spark):
    """Rule 3: All fields in Fact table must be present in dim_field."""
    fct_df = spark.read.format("delta").load("data/gold/fct_monthly_production")
    dim_df = spark.read.format("delta").load("data/gold/dim_field")

    # Anti-join to identify orphan records
    orphans = fct_df.join(dim_df, on="field_name", how="left_anti").count()

    assert (
        orphans == 0
    ), f"Found {orphans} fact records missing dimension metadata!"