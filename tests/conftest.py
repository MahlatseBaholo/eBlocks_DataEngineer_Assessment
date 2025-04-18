import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("Test Suite") \
        .master("local[*]") \
        .config("spark.driver.extraClassPath", "/extra-jars/mysql-connector-j-8.0.33.jar") \
        .config("spark.executor.extraClassPath", "/extra-jars/mysql-connector-j-8.0.33.jar") \
        .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/northwind.customers") \
        .getOrCreate()
