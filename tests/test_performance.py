import time

def test_mysql_read_time(spark):
    start = time.time()
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://mysql:3306/northwind") \
        .option("dbtable", "customers") \
        .option("user", "user") \
        .option("password", "pass") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()
    end = time.time()

    assert (end - start) < 5  # e.g., 5 seconds max for small datasets
