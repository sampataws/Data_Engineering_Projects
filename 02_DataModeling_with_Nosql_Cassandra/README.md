# Data Modeling with Cassandra

## **Overview**
In this project, I would be applying Data Modeling with Apache Cassandra and complete an ETL pipeline using Python. I am provided with part of the ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables.

## **Dataset**
For this project, you'll be working with one dataset: event_data. 
The directory of CSV files partitioned by date. 
Here are examples of filepaths to two files in the dataset: event_data/2018-11-08-events.csv event_data/2018-11-09-events.csv

## **Project Steps:**

Below are steps you can follow to complete each component of this project.

```Modelling your NoSQL Database or Apache Cassandra Database:```

* Design tables to answer the queries outlined in the project template
* Write Apache Cassandra CREATE KEYSPACE and SET KEYSPACE statements
* Develop your CREATE statement for each of the tables to address each question
* Load the data with INSERT statement for each of the tables
* Include IF NOT EXISTS clauses in your CREATE statements to create tables only if the tables do not already exist. We recommend you also include DROP TABLE statement for each table, this way you can run drop and create tables whenever you want to reset your database and test your ETL pipeline
* Test by running the proper select statements with the correct WHERE clause


## Build ETL Pipeline:

1. Implement the logic in section Part I of the notebook template to iterate through each event file in event_data to process and create a new CSV file in Python
2. Make necessary edits to Part II of the notebook template to include Apache Cassandra CREATE and INSERT three statements to load processed records into relevant tables in your data model
3. Test by running three SELECT statements after running the queries on your database
4. Finally, drop the tables and shutdown the cluster

###How to Run
* ```MODULE_NAME``` = 02_DataModeling_with_Nosql_Cassandra
* ```PACKAGE_NAME``` = com.sampat.de.datamodel.cassandra

```Main Execution Python File:```
* Implemeted a logger which will log into a file("app_log.log") under the directory -> ```02_DataModeling_with_Nosql_Cassandra/log```
* Main file to execute the run is -> ```etl.py``` . NOTE : The file exists under the module ```02_DataModeling_with_Nosql_Cassandra``` with package name as ```com.sampat.de.datamodel.cassandra```
