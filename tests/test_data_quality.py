def test_no_null_emails(spark):
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://mysql:3306/northwind") \
        .option("dbtable", "customers") \
        .option("user", "user") \
        .option("password", "pass") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()

    null_email_count = df.filter("email IS NULL").count()
    assert null_email_count == 0
