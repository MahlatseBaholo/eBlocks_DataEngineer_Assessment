# eBlocks_DataEngineer_Assessment

# Docker

1. Script 1- Create a docker script to start an existing mysql docker instance.
   
   Scripts are placed under the root folder: Dockerfile, docker-compose.yml
   
3. Script 2- Create a script to import the orders details information into the mysql database.

   Scripts are placed under the sql folder: create_schema_and_tables.sql, insert_data.sql
   
5. Script 3- Write a Scala or Python Spark or scala script that would do the following:
   
   a. Predict the next order date for a customer, based on his purchase patterns.

   b. Only customers predicted to buying products in the next week( 7 days) must be placed in the mongo database.

      The main script is placed under the app folder: predict_next_order_date.py
      
      Dependency file placed in root folder: requirements.txt
   
      JARS are placed under the jars folder: mongo-spark-connector_2.12-10.4.1-all.jar, mysql-connector-j-8.0.33.jar
      
      Dockerfile for testing placed in the root folder: Dockerfile.test
   
      Testing scripts are placed in the tests folder: test_integration.py, test_data_quality.py, test_integration.py, test_performance.py, 
      test_prediction_output.py
   
7. Explain under which conditions you would use MySQL and Mongo?

   MySQL: I would use MySQL because I’m working with structured, relational data that follows a well-defined schema. MySQL is ideal for handling normalized data with clear relationships (e.g., foreign keys) and is well-suited for operations like joins, aggregations, and complex queries. It also ensures data integrity through ACID compliance, which is important when working with transactional data such as customer orders and purchase history.
   
   MongoDB: I would use MongoDB because I’m dealing with semi-structured or evolving data where the schema isn’t fixed and is expected to change over time. MongoDB offers flexibility in how data is modeled and supports horizontal scalability, making it a good fit for large datasets. It's optimized for fast read/write performance, especially in use cases involving high volumes of data, such as storing prediction 
   results that don't necessarily fit into a strict relational model.
   
8. Explain your choice in context of the CAP Theorem?
   MySQL (CA – Consistency and Availability)
   
   MySQL, being a traditional relational database, prioritizes:
   
   •	Consistency: Every read receives the most recent write. MySQL enforces ACID (Atomicity, Consistency, Isolation, Durability) properties,          which ensure that transactions are reliable and data remains in a valid state.
   
   •	Availability: MySQL is designed to be reliably available on single-node or master-slave architectures, meaning the system remains                responsive as long as there's no network partition.

   •	Partition Tolerance: MySQL does not natively prioritize partition tolerance. In distributed systems, if a network failure happens between        nodes, MySQL will typically sacrifice availability to preserve consistency. This makes it less ideal for large, distributed environments         with potential network issues.

   In this case, working with structured, transactional data where data integrity and accuracy matter most MySQL’s CA traits make it a strong       fit.

   MongoDB (CP – Consistency and Partition Tolerance)
   
   MongoDB, as a NoSQL database, emphasizes:
   
   •	Partition Tolerance: MongoDB is built to work in distributed environments. It handles network splits and still tries to function, though         certain trade-offs may apply.
   
   •	Consistency: MongoDB can be strongly consistent by default in a replica set—reads and writes go to the primary node unless configured             otherwise.
   
   •	Availability: In the face of partitions, MongoDB may sacrifice availability (e.g., if no primary is available in a replica set) to ensure        consistency and avoid stale reads or writes.
   
   In this case, MongoDB’s CP characteristics makes it ideal when the data model is more flexible, evolves over time, and scales horizontally.

9. Explain and draw a diagram on how you would design the above architecture in the cloud (Azure or AWS).
    
   a. What technologies would you use in the cloud stack?
   
   b. How would you move data from on premises (MySQL) to the cloud.
   
      •	Since current architecture does not have lot of tables, complex relationships, no indexes, I will use Azure Database for MySQL as the target database and perform incremental data migration.
   
      •	I will setup the Azure Database for MySQL, implement Azure Virtual Network (VNet) for secure network connectivity and a VPN Gateway to establish a secure, low-latency connection between on-premises and Azure. Enable Azure Active Directory (AAD) for authentication and secure access to the cloud resources. Set up Azure Key Vault to securely manage database credentials and encryption keys.
   
      •	I will use the lift and shit method to move the entire MySQL database to the cloud using Azure Database Migration Service (DMS).
   
      •	I will ensure that the data has been accurately migrated, verifying that it matches the original data and adheres to the required schema by following below steps.
   
      •	Plan for a cutover window during off-peak hours to minimize impact on users.
   
      •	Following change management, I will decommission the on-premise database.
   
      •	Set up Azure Monitor to track database performance and operational health. Use Azure Security Center to continuously monitor for any security vulnerabilities. Implement automated backups, data    encryption, and other security measures to ensure the integrity and safety of data in the cloud. Schedule regular database maintenance tasks (e.g., indexing, query optimization).
   
      •	Regularly review cloud usage with Azure Cost Management. Optimize cloud resources based on usage patterns, scaling up or down where appropriate.



You must provide detailed files for script 1, 2 and 3.

# Black Friday 

On the day ofBlackFriday, you realize that you have a large number of orders, explain how you would make changes to your data engineering architecture to be more robust, scalable, reliable and real time.

1. What technologies would you use?
   
   Ingestion Layer: Kafka
   
   Processing Layer: Azure Databricks
   
   Storage Layer: Azure Database for MySQL Server, Azure Cosmos DB
   
   Analytics Layer: Power BI

   Monitoring: Azure Monitor

   Security: Azure Managed Indenties, Azure Key Vault
   
3. Why would you choose these technologies?
   a)	Reliability (Resilience and Fault Tolerance)

      Goal: Ensure the system can recover gracefully from failures and continue functioning during unexpected loads.
   
      Improvements:
   
      •	Use Kafka as an ingestion layer: Decouples producers (orders, apps) from consumers (pipelines), buffering incoming events and 
         protecting downstream services.
      
      •	Introduce retry logic & dead-letter queues (DLQs) in my ingestion.
   
      •	Deploy Spark on a Databricks managed cluster: This service automatically handle failures and retries better than self-managed Spark in 
         Docker.

   b) Scalability (Horizontal & Elastic Scaling)
   
      Goal: Automatically handle growing data and traffic without manual intervention.
      Improvements:
   
      •	Use Databricks jobs to scale compute on demand.
   
      •	Partition MySQL/MongoDB datasets by state to reduce contention and improve parallel processing.
   
      •	Optimize PySpark with partitioning, caching, and predicate pushdown to make better use of resources.
   
      •	Use Azure Cosmos DB instead of raw MongoDB if my workloads are truly global — Cosmos handles autoscaling and global distribution 
         natively.

    c) Performance Efficiency (Low Latency & Fast Processing)
   
      Goal: Deliver insights and responses in near-real-time.
   
      Improvements:
   
      •	Switch to streaming ETL with PySpark Structured Streaming.
   
      •	Use Delta Lake on Databricks to enable fast, atomic reads/writes for massive datasets.
   
      •	Move toward real-time dashboards using Power BI with DirectQuery on my Cosmos DB layers.

      d)	Security (Data Protection & Access Control)
   
      Goal: Secure data at rest and in motion, and control access tightly during peak events.
   
      Improvements:
   
      •	Use Managed Identities and Azure Key Vault to securely manage credentials for MySQL/MongoDB access.
   
      •	Implement row-level or column-level security for sensitive data in Databricks.

      e)	Cost Optimization
   
      Goal: Minimize unnecessary spend while still meeting performance and availability needs.
   
      Improvements:
      •	Analyze pipeline logs to reduce redundant transformations or unnecessary storage.

5. What data patterns would you use ?
   
   •	Event-driven Architecture: Implement an event-driven architecture using Apache Kafka. Orders can be published as events to Kafka       
      topics, enabling real-time processing and analysis.
   
   •	Stream Processing: Utilize Apache Spark's streaming capabilities to process the order data in real-time, perform aggregations, 
      enrichments, and apply business rules.
   
   •	Microservices Architecture: Decompose the system into microservices that can independently handle different aspects of order processing, 
      such as inventory management, payment processing, and shipping.
   
   •	Caching: Use in-memory caching technologies like Azure Cache for Redis to cache frequently accessed data, such as customer information, 
      product details, and pricing information, to improve response times and reduce database load.
   
   •	Batch Processing: Utilize Apache Spark to process historical order data and generate insights. This can help in analyzing trends, 
      customer behaviour, and planning for future Black Friday events.
   
   •	Monitoring and Alerting: Implement monitoring and alerting mechanisms to detect anomalies, performance bottlenecks, and failures in the 
      data processing pipeline. Tools like Grafana can be used for monitoring and visualization.
    
# Order Analysis

Please provide scripts and results for the following:

1. Which day of the week has the most orders?
   
   Saturday with 97 orders.
   
3. Which time of the day do people order the most?

   12 AM with 600 orders.

4. Which order does the user buy first most of the time?

   Canon T90 has been purchased 17 times by users as a first-time product.

5. What is the time interval that a user tends to purchase again?
   
   It takes on average 49 days for a user to make another purchase.

7. Write a mysql script on how to delete the duplicate orders, of the latest date, please explain the script in detail?
   
   3 records where deleted.
   
   The script drops the existing foreign key constraint that prevents deletion of an orders record since it's referenced in order_details.
   Adds a new foreign key constraint with ON DELETE CASCADE, which means that when an order is deleted from the orders table, all related rows in the order_details table will be automatically deleted.
   
   Without this, trying to delete orders that have order details would fail with a foreign key constraint error. Cascading deletes allow safe cleanup without needing to manually delete from order_details first.

