# eBlocks_DataEngineer_Assessment

1. Script 1- Create a docker script to start an existing mysql docker instance.
2. Script 2- Create a script to import the orders details information into the mysql database.
3. Script 3- Write a Scala or Python Spark or scala script that would do the following:
   
   a. Predict the next order date for a customer, based on his purchase patterns.
   
   b. Only customers predicted to buying products in the next week( 7 days) must be placed in the mongo database.
5. Explain under which conditions you would use MySQL and Mongo?

   MySQL: I would use MySQL because I’m working with structured, relational data that follows a well-defined schema. MySQL is ideal for handling normalized data with clear relationships (e.g., foreign keys) and is well-suited for operations like joins, aggregations, and complex queries. It also ensures data integrity through ACID compliance, which is important when working with transactional data such as customer orders and purchase history.
   
   MongoDB: I would use MongoDB because I’m dealing with semi-structured or evolving data where the schema isn’t fixed and is expected to change over time. MongoDB offers flexibility in how data is modeled and supports horizontal scalability, making it a good fit for large datasets. It's optimized for fast read/write performance, especially in use cases involving high volumes of data, such as storing prediction 
   results that don't necessarily fit into a strict relational model.
   
7. Explain your choice in context of the CAP Theorem?
   MySQL (CA – Consistency and Availability)
   MySQL, being a traditional relational database, prioritizes:
   •	Consistency: Every read receives the most recent write. MySQL enforces ACID (Atomicity, Consistency, Isolation, Durability) properties,          which ensure that transactions are reliable and data remains in a valid state.
   •	Availability: MySQL is designed to be reliably available on single-node or master-slave architectures, meaning the system remains                responsive as long as there's no network partition.

   In this case, working with structured, transactional data where data integrity and accuracy matter most MySQL’s CA traits make it a strong       fit.

9. Explain and draw a diagram on how you would design the above architecture in the cloud (Azure or AWS).
   a. What technologies would you use in the cloud stack?
   b. How would you move data from on premises (MySQL) to the cloud.

You must provide detailed files for script 1, 2 and 3.

# Black Friday 

On the day ofBlackFriday, you realize that you have a large number of orders, explain how you would make changes to your data engineering architecture to be more robust, scalable, reliable and real time.

1. What technologies would you use?
2. Why would you choose these technologies?
3. What data patterns would you use ?
  
# Order Analysis

Please provide scripts and results for the following:

1. Which day of the week has the most orders?
2. Which time of the day do people order the most?
3. Which order does the user buy first most of the time?
4. What is the time interval that a user tends to purchase again?
5. Write a mysql script on how to delete the duplicate orders, of the latest date, please explain the script in detail?
