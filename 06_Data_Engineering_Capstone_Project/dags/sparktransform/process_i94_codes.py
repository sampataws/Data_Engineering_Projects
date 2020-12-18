from  plugins.utils.constants import S3Buckets as s3bucket,S3AllImmigrationDataKeyPaths as s3keypath
import pyspark.sql.functions as F
import os


aws_s3_bucket_name = "s3a://{}".format(s3bucket.UDACITY_DE_PROJECT_BUCKET_NAME)
i94_codes_s3_key_path = s3keypath.S3_IMMIGRATION_DATA_LABEL_CODE_STAGING_KEY_PATH
i94_processed_airport_code_key_path = s3keypath.S3_IMMIGRATION_DATA_CODES_AIRPORT_PROCESSED_KEY_PATH
i94_processed_country_code_key_path = s3keypath.S3_IMMIGRATION_DATA_CODES_COUNTRY_PROCESSED_KEY_PATH
i94_processed_state_code_key_path = s3keypath.S3_IMMIGRATION_DATA_CODES_STATE_PROCESSED_KEY_PATH
i94_processed_model_code_key_path = s3keypath.S3_IMMIGRATION_DATA_CODES_MODE_PROCESSED_KEY_PATH
i94_processed_visa_code_key_path = s3keypath.S3_IMMIGRATION_DATA_CODES_VISA_PROCESSED_KEY_PATH




airport_code_file_name = "i94port.csv"
country_code_file_name = "i94cit_i94res.csv"
state_code_file_name = "i94addr.csv"
model_code_file_name = "i94mode.csv"
visa_code_file_name = "i94visa.csv"

#Process and Clean Bad codes:

def process_airport_codes():
    s3_inpath_for_airport_code = os.path.join(aws_s3_bucket_name,i94_codes_s3_key_path,airport_code_file_name)
    """ Note that we don’t need to import and create the spark session as it’s already provided as spark by the 
        spark shell while creating spark session via Livy ."""
    df_airport = spark.read.format("csv").option("delimiter", ",").option("header", "true").load(s3_inpath_for_airport_code)
    df_airport = df_airport.withColumn("port_of_entry",F.regexp_replace(df_airport.port_of_entry, "^No Port Code.*|^INVALID.*|Collapsed.*|No\ Country.*","INVALID")).\
        withColumn("city",F.regexp_replace(df_airport.city, "^No Port Code.*|^INVALID.*|Collapsed.*|No\ Country.*","INVALID")).\
        filter("state_or_country is not null")
    df_airport = df_airport.withColumnRenamed("code","airport_code")
    df_airport.write.mode("overwrite").parquet("{}{}".format(s3bucket,i94_processed_airport_code_key_path))

def process_state_codes():
    s3_inpath_for_state_code = os.path.join(aws_s3_bucket_name,i94_codes_s3_key_path,state_code_file_name)
    df_state = spark.read.format("csv").option("delimiter", ",").option("header", "true").load(s3_inpath_for_state_code)
    df_state = df_state.withColumnRenamed("state","state_code")
    df_state.write.mode("overwrite").parquet("{}{}".format(s3bucket,i94_processed_state_code_key_path))

def process_country_codes():
    s3_inpath_for_country_code = os.path.join(aws_s3_bucket_name,i94_codes_s3_key_path,country_code_file_name)
    df_country = spark.read.format("csv").option("delimiter", ",").option("header", "true").load(s3_inpath_for_country_code)
    df_country = df_country.withColumn("country",F.regexp_replace(df_country.country, "^No Port Code.*|^INVALID.*|Collapsed.*|No\ Country.*","INVALID"))
    #filter all the invalid codes
    df_country = df_country.filter("country != 'INVALID'")
    df_country.write.mode("overwrite").parquet("{}{}".format(s3bucket,i94_processed_country_code_key_path))

def process_model_codes():
    s3_inpath_for_model_code = os.path.join(aws_s3_bucket_name,i94_codes_s3_key_path,model_code_file_name)
    df_model = spark.read.format("csv").option("delimiter", ",").option("header", "true").load(s3_inpath_for_model_code)
    df_model.write.mode("overwrite").parquet("{}{}".format(s3bucket,i94_processed_model_code_key_path))

def process_visa_codes():
    s3_inpath_for_visa_code = os.path.join(aws_s3_bucket_name, i94_codes_s3_key_path, model_code_file_name)
    df_visa = spark.read.format("csv").option("delimiter", ",").option("header", "true").load(s3_inpath_for_visa_code)
    df_visa.write.mode("overwrite").parquet("{}{}".format(s3bucket, i94_processed_visa_code_key_path))









