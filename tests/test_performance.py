import time
from pyspark.sql import SparkSession

# Initialize Spark session
def get_spark_session():
    return SparkSession.builder \
        .appName("MySQLPerformanceTest") \
        .config("spark.driver.extraClassPath", "/extra-jars/mysql-connector-j-8.0.33.jar") \
        .config("spark.executor.extraClassPath", "/extra-jars/mysql-connector-j-8.0.33.jar") \
        .config("spark.jars", "/extra-jars/mysql-connector-j-8.0.33.jar") \
        .getOrCreate()

def test_read_performance():
    spark = get_spark_session()

    # Start time measurement
    start_time = time.time()

    # Load data from MySQL
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://mysql:3306/northwind") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "customers") \
        .option("user", "user") \
        .option("password", "rootpass") \
        .load()

    # End time measurement
    end_time = time.time()

    # Calculate time taken
    time_taken = end_time - start_time
    print(f"Time taken to read data from MySQL: {time_taken:.2f} seconds")

    # Assert that the time is within the acceptable range
    acceptable_time = 2  # 2 seconds
    assert time_taken <= acceptable_time, f"Read took too long: {time_taken:.2f} seconds"

# Run the test
if __name__ == "__main__":
    test_read_performance()
