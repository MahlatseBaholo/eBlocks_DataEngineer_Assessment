import pytest
from pyspark.sql import SparkSession

# --- SparkSession Fixture ---
@pytest.fixture(scope="session")
def spark():
    # Stop any existing Spark session to avoid conflicts
    try:
        if 'spark' in globals() and spark is not None:
            print("Stopping existing Spark session...")
            spark.stop()
    except Exception as e:
        print(f"Error while stopping existing Spark session: {e}")
    
    # Initialize a new Spark session
    print("Initializing a new Spark session...")
    spark = SparkSession.builder \
        .appName("Test Suite") \
        .master("local[*]") \
        .config("spark.driver.extraClassPath", "/extra-jars/mysql-connector-j-8.0.33.jar") \
        .config("spark.executor.extraClassPath", "/extra-jars/mysql-connector-j-8.0.33.jar") \
        .config("spark.jars", "/extra-jars/mysql-connector-j-8.0.33.jar") \
        .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/northwind.customers") \
        .getOrCreate()
    
    yield spark
    
    # Stop the Spark session after the test session completes
    print("Stopping Spark session...")
    spark.stop()

# --- Test Cases ---
def test_mysql_jar_loaded(spark):
    """Check if MySQL JDBC driver JAR is configured correctly."""
    jars = spark.conf.get("spark.jars", "")
    driver_classpath = spark.conf.get("spark.driver.extraClassPath", "")
    executor_classpath = spark.conf.get("spark.executor.extraClassPath", "")
    
    mysql_jar_path = "/extra-jars/mysql-connector-j-8.0.33.jar"
    
    assert mysql_jar_path in jars or mysql_jar_path in driver_classpath or mysql_jar_path in executor_classpath, \
        "MySQL JAR not found in Spark configuration"

def test_mongodb_uri_set(spark):
    """Ensure MongoDB write URI is configured."""
    mongodb_uri = spark.conf.get("spark.mongodb.write.connection.uri", "")
    assert mongodb_uri.startswith("mongodb://mongodb:27017/"), \
        f"MongoDB URI not set correctly: {mongodb_uri}"
