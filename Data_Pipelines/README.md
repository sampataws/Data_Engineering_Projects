# Summary of project

Startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In order to enable Sparkify to parse their data automated over S3 into Redshift, they decided to use Airflow to schedule and monitor their pipeline jobs.

# Files in the repository

* **[workspace](airflow)**: workspace folder storing the airflow DAGs and plugins used by the airflow server.
* **[workspace/dags/from_s3_to_redshift_dag.py](airflow/dags/s3_to_redshift_dag.py)**: "Main"-DAG written in Python containing all steps of the pipeline.
* **[workspace/plugins/operators/](airflow/plugins/operators)**: Folder containing such called operators which can be called inside an Airflow DAG. These are python classes to outsource and build generic functions as modules of the DAG.


#Datasets
For this project, you'll be working with two datasets. Here are the s3 links for each:

* Log data: ```s3://udacity-dend/log_data```
* Song data: ```s3://udacity-dend/song_data```