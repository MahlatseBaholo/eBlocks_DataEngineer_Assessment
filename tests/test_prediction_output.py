import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType
from pyspark.sql.functions import col

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("TestPredictionPipeline").getOrCreate()

def test_predicted_order_dates_valid(spark):
    # Example: mock prediction data or use actual dataframe
    data = [
        (1, datetime(2023, 9, 1), datetime(2023, 9, 8)),
        (2, datetime(2023, 8, 15), datetime(2023, 8, 22)),
    ]
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("order_date", TimestampType(), True),
        StructField("predicted_order_date", TimestampType(), True),
    ])
    df = spark.createDataFrame(data, schema)

    # Check that prediction is not null
    assert df.filter(col("predicted_order_date").isNull()).count() == 0, "Null predictions found"

    # Check prediction is after order_date
    assert df.filter(col("predicted_order_date") <= col("order_date")).count() == 0, "Predicted date before or equal to last order date"

    # Check if all predicted_order_date are datetime instances
    rows = df.collect()
    for row in rows:
        assert isinstance(row.predicted_order_date, datetime), "Prediction is not datetime object"
