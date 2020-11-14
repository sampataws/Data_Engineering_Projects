#Udacity Data Engineering Capstone Project

##Project Summary

The scope of this project is to explore United States immigration data. And specifically we would like to gather below insigts :
* To Analyze where the visitors travel from and to which cities ?
* Explore the demographics  of the cities travelled ?
* Monthly and Weekly average temperatures of these cities ?
* Effect of temperature on the scale of travellers ?
* Identifying the relationship between volume of travel and no of entry points (i:e airports)
* Indetifying the relationship between volumns of travel and various city demographics .

##DataSets Used 
* **I94 Immigration Data**: This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. [This](https://travel.trade.gov/research/reports/i94/historical/2016.html) is where the data comes from. There's a sample file so you can take a look at the data in csv format before reading it all in. You do not have to use the entire dataset, just use what you need to accomplish the goal you set at the beginning of the project.
* **World Temperature Data**: This dataset came from Kaggle. You can read more about it [here](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).
* **U.S. City Demographic Data**: This data comes from OpenSoft. You can read more about it [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).
* **Airport Code Table**: This is a simple table of airport codes and corresponding cities. It comes from [here](https://datahub.io/core/airport-codes#data). 

**NOTE**: To achieve this we will use AWS EMR, Apache airflow, Apache Livy and Pyspark.


##Scope of the Project
1. Gather all the data
2. Explore and Assess the data
3. Define Data Model
4. Run ERL to Model the data
5. Complete project Write Up

##Environment Setup On AWS Using CloudFormation
For AWS cloud setup we are going to use cloud formation script. The scripts will setup , Airflow Instances in EC2 , Postgres DB for Metastore and AWS EMR with Livy for Spark processing.
For more details on how to create a cloudformation script refer to [this](https://aws.amazon.com/blogs/big-data/build-a-concurrent-data-orchestration-pipeline-using-amazon-emr-and-apache-livy/) link.







