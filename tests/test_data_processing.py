import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
from src.main import calculate_max_amount_per_day, calculate_sum_amount_per_atm, preprocess_data

@pytest.fixture(scope="session")
def spark():
    """Fixture for creating a Spark session."""
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("pytest-pyspark-local-testing") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_preprocess_data(spark):
    """Test the preprocess_data function."""
    data = [("2021-04-11 09:09:09", "00321", 600.00),
            ("2021-04-11 11:12:13", "00321", 300.00)]
    schema = ["timestamp", "atm_id", "amount"]
    df = spark.createDataFrame(data, schema)
    processed_df = preprocess_data(df)
    assert processed_df.filter(col("date") == datetime.strptime("2021-04-11", "%Y-%m-%d").date()).count() == 2

def test_calculate_max_amount_per_day(spark):
    """Test the calculate_max_amount_per_day function."""
    data = [("2021-04-11", 600.00),
            ("2021-04-11", 300.00),
            ("2021-04-12", 200.00)]
    schema = ["date", "amount"]
    df = spark.createDataFrame(data, schema)
    result_df = calculate_max_amount_per_day(df)
    expected_data = [("2021-04-11", 600.00),
                     ("2021-04-12", 200.00)]
    expected_df = spark.createDataFrame(expected_data, schema)
    assert result_df.collect() == expected_df.collect()

def test_calculate_sum_amount_per_atm(spark):
    """Test the calculate_sum_amount_per_atm function."""
    data = [("00321", 600.00),
            ("00321", 300.00),
            ("00248", 150.00)]
    schema = ["atm_id", "amount"]
    df = spark.createDataFrame(data, schema)
    result_df = calculate_sum_amount_per_atm(df)
    expected_data = [("00321", 900.00),
                     ("00248", 150.00)]
    expected_schema = ["atm_id", "sum_amount"]
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    assert result_df.collect() == expected_df.collect()

# Add more tests as necessary for other functions or edge cases.
