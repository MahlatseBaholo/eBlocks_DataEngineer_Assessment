import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("Data Quality Test") \
        .config("spark.driver.extraClassPath", "/extra-jars/mysql-connector-j-8.0.33.jar") \
        .config("spark.executor.extraClassPath", "/extra-jars/mysql-connector-j-8.0.33.jar") \
        .config("spark.jars", "/extra-jars/mysql-connector-j-8.0.33.jar") \
        .getOrCreate()

def test_null_values_in_customers_and_orders(spark):
    # Load the customers table
    customers_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://mysql:3306/northwind") \
        .option("dbtable", "customers") \
        .option("user", "user") \
        .option("password", "rootpass") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()

    # Load the orders table
    orders_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://mysql:3306/northwind") \
        .option("dbtable", "orders") \
        .option("user", "user") \
        .option("password", "rootpass") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()

    # Check that customer_id in customers table has no null values
    null_count_customers = customers_df.filter(col("id").isNull()).count()
    assert null_count_customers == 0, f"Column 'customer_id' in 'customers' contains {null_count_customers} null values"

    # Check that order_date in orders table has no null values
    null_count_orders = orders_df.filter(col("order_date").isNull()).count()
    assert null_count_orders == 0, f"Column 'order_date' in 'orders' contains {null_count_orders} null values"
