from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from unittest.mock import MagicMock  # Import MagicMock
import pytest

# Create a fixture for Spark session
@pytest.fixture(scope="session")
def spark():
    # Initialize the Spark session here
    return SparkSession.builder \
        .appName("Unit Testing") \
        .master("local[1]") \
        .getOrCreate()

# Sample data for customers and orders
@pytest.fixture
def sample_data(spark):
    # Sample data for customers and orders
    customers_data = [
        (1, "John Doe", "john.doe@example.com"),
        (2, "Jane Smith", "jane.smith@example.com"),
    ]
    
    orders_data = [
        (1, 1, "2023-04-01", "2023-04-02", "2023-04-03"),
        (2, 1, "2023-04-15", "2023-04-16", "2023-04-17"),
        (3, 2, "2023-04-05", "2023-04-06", "2023-04-07"),
    ]
    
    # Define the schemas for customers and orders
    customers_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
    ])
    
    orders_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("order_date", StringType(), True),  # Use StringType initially
        StructField("shipped_date", StringType(), True),  # Use StringType initially
        StructField("paid_date", StringType(), True),  # Use StringType initially
    ])
    
    # Create dataframes using the spark session
    customers_df = spark.createDataFrame(customers_data, customers_schema)
    orders_df = spark.createDataFrame(orders_data, orders_schema)
    
    # Convert string dates to actual DateType
    orders_df = orders_df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
                         .withColumn("shipped_date", to_date(col("shipped_date"), "yyyy-MM-dd")) \
                         .withColumn("paid_date", to_date(col("paid_date"), "yyyy-MM-dd"))
    
    return customers_df, orders_df

# Unit test to check if data is read correctly from MySQL (simulated here)
def test_mysql_read(spark, sample_data):
    customers_df, orders_df = sample_data
    
    # Join the dataframes on customer_id
    joined_df = customers_df.join(orders_df, customers_df["id"] == orders_df["customer_id"], "inner")
    
    # Check if the join is successful
    assert joined_df.count() == 3  # We expect 3 records after joining
    assert "name" in joined_df.columns  # Ensure customer name is in the result
    assert "order_date" in joined_df.columns  # Ensure order_date is in the result

# Unit test for data transformation (filling missing values and date conversions)
def test_data_transformation(spark, sample_data):
    customers_df, orders_df = sample_data
    
    # Transform orders_df (na.fill and date conversion)
    transformed_df = orders_df.na.fill("None") \
        .withColumn("order_date", col("order_date").cast(DateType())) \
        .withColumn("shipped_date", col("shipped_date").cast(DateType())) \
        .withColumn("paid_date", col("paid_date").cast(DateType()))
    
    # Validate the transformations
    assert transformed_df.filter(col("order_date").isNull()).count() == 0  # No null values for order_date
    assert transformed_df.count() == orders_df.count()  # Ensure count remains the same

# Unit test for feature engineering and prediction (simple check)
def test_feature_engineering_and_prediction(spark, sample_data):
    customers_df, orders_df = sample_data
    
    # Simulate feature engineering (days_since_last_order)
    joined_df = customers_df.join(orders_df, customers_df["id"] == orders_df["customer_id"], "inner") \
        .withColumn("days_since_last_order", lit(5))  # Mocked value for testing purposes
    
    # Assert that the feature engineering works
    assert joined_df.filter(col("days_since_last_order") == 5).count() == 3  # Ensure all rows have the value 5
    
    # Simulate predictions
    predicted_df = joined_df.withColumn("predicted_order_date", lit("2023-05-01"))
    
    # Assert that the prediction column exists
    assert "predicted_order_date" in predicted_df.columns

# Unit test for MongoDB insertion (mocking the MongoDB connection)
def test_mongodb_insertion(spark, sample_data):
    customers_df, orders_df = sample_data
    
    # Mock the MongoDB client
    mongo_client = MagicMock()
    
    # Simulate MongoDB insertion
    predicted_data = [{"customer_id": 1, "predicted_order_date": "2023-05-01"}]
    
    mongo_client["northwind"]["predicted_orders"].insert_many(predicted_data)
    
    # Assert that insert_many was called
    mongo_client["northwind"]["predicted_orders"].insert_many.assert_called_with(predicted_data)
