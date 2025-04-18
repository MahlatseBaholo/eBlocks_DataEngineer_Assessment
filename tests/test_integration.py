from pymongo import MongoClient

def test_data_write_to_mongodb(spark):
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://mysql:3306/northwind") \
        .option("dbtable", "customers") \
        .option("user", "user") \
        .option("password", "pass") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()

    df.write \
        .format("mongo") \
        .mode("overwrite") \
        .option("uri", "mongodb://mongodb:27017/northwind.customers_test") \
        .save()

    client = MongoClient("mongodb://mongodb:27017")
    db = client["northwind"]
    count = db["customers_test"].count_documents({})

    assert count == df.count()
