from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, datediff, to_date, lag, coalesce, lit,
    unix_timestamp, from_unixtime, max as spark_max, when
)
from pyspark.sql.window import Window
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from datetime import datetime, timedelta
from pymongo import MongoClient

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MySQL to MongoDB") \
    .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/northwind.predicted_orders") \
    .getOrCreate()

# Define MySQL connection properties
mysql_url = "jdbc:mysql://mysql:3306/northwind"

mysql_properties = {
    "user": "user",
    "password": "rootpass",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read data from MySQL
customers_df = spark.read.jdbc(mysql_url, "customers", properties=mysql_properties)
orders_df = spark.read.jdbc(mysql_url, "orders", properties=mysql_properties)

# Join tables
joined_df = customers_df.join(
    orders_df,
    customers_df["id"] == orders_df["customer_id"],
    "inner"
)

# Fill missing and convert dates
joined_df = joined_df.na.fill("None") \
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
    .withColumn("shipped_date", to_date(col("shipped_date"), "yyyy-MM-dd")) \
    .withColumn("paid_date", to_date(col("paid_date"), "yyyy-MM-dd"))

# Window spec to calculate gaps between orders
window_spec = Window.partitionBy("customer_id").orderBy("order_date")
joined_df = joined_df.withColumn("previous_order_date", lag("order_date").over(window_spec)) \
                     .withColumn("days_since_last_order", datediff(col("order_date"), col("previous_order_date")))

# Prepare latest order per customer
latest_orders = joined_df.withColumn(
    "max_order_date",
    spark_max("order_date").over(Window.partitionBy("customer_id"))
).filter(col("order_date") == col("max_order_date")) \
 .select("customer_id", "days_since_last_order", "order_date") \
 .withColumn("days_since_last_order", coalesce(col("days_since_last_order"), lit(0))) \
 .withColumn("order_date_numeric", unix_timestamp("order_date").cast("double"))

# Prepare full historical data for training
training_data = joined_df.select("customer_id", "days_since_last_order", "order_date") \
    .withColumn("days_since_last_order", coalesce(col("days_since_last_order"), lit(0))) \
    .withColumn("order_date_numeric", unix_timestamp("order_date").cast("double"))

# Feature engineering
assembler = VectorAssembler(inputCols=["days_since_last_order"], outputCol="features", handleInvalid="skip")
training_data = assembler.transform(training_data)
latest_orders = assembler.transform(latest_orders)

# Train model
lr = LinearRegression(featuresCol="features", labelCol="order_date_numeric")
model = lr.fit(training_data.select("features", "order_date_numeric"))

# Predict next order date
predictions = model.transform(latest_orders)

# Ensure prediction is after the latest order
predictions = predictions.withColumn(
    "adjusted_prediction",
    when(
        col("prediction") <= col("order_date_numeric"),
        col("order_date_numeric") + lit(86400)  # Add 1 day (86400 seconds)
    ).otherwise(col("prediction"))
).withColumn(
    "predicted_order_date",
    from_unixtime(col("adjusted_prediction")).cast("timestamp")
)

# Show one prediction per customer
predictions.select("customer_id", "order_date", "predicted_order_date").show(truncate=False)
# Filter predictions within the next 7 days
current_date = datetime.now()
next_week = current_date + timedelta(days=7)
next_week_timestamp = next_week.timestamp()

predicted_customers = predictions.filter(predictions.prediction <= next_week_timestamp)

# Convert prediction timestamp to readable date
clean_predictions = predicted_customers.select(
    "customer_id",
    from_unixtime(col("prediction")).cast("timestamp").alias("predicted_order_date")
)

# Convert to list of dictionaries for MongoDB
predicted_customers_data = clean_predictions.toPandas().to_dict(orient="records")

# Insert predictions into MongoDB
if predicted_customers_data:
    mongo_uri = "mongodb://mongodb:27017/"
    client = MongoClient(mongo_uri)
    db = client["northwind"]
    collection = db["predicted_orders"]
    collection.insert_many(predicted_customers_data)
    client.close()

# Optional: Show results in console
clean_predictions.show()

# Stop Spark
spark.stop()
