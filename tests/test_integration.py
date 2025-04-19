import pytest
from pyspark.sql import SparkSession

# --- SparkSession Fixture ---
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .appName("Test Suite") \
        .master("local[*]") \
        .config("spark.driver.extraClassPath", "/extra-jars/mysql-connector-j-8.0.33.jar") \
        .config("spark.executor.extraClassPath", "/extra-jars/mysql-connector-j-8.0.33.jar") \
        .config("spark.jars", "/extra-jars/mysql-connector-j-8.0.33.jar") \
        .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/northwind.customers") \
        .getOrCreate()
    
    yield spark
    spark.stop()

# --- Test Cases ---
def test_spark_session_is_initialized(spark):
    """Ensure SparkSession is created and running."""
    assert spark is not None
    assert spark.sparkContext is not None
    assert not spark.sparkContext._jsc.sc().isStopped()
    assert spark.sparkContext.appName == "Test Suite"


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
