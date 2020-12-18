import os

from  plugins.utils.constants import S3Buckets as s3bucket,S3AllImmigrationDataKeyPaths as s3keypath

aws_s3_bucket_name = "s3a://{}".format(s3bucket.UDACITY_DE_PROJECT_BUCKET_NAME)
demographics_data_file_name = "us-cities-demographics.csv"
demographics_data_s3_keypath = s3keypath.S3_IMMIGRATION_DATA_DEMOGRAPHICS_STAGING_KEY_PATH
demographics_data_s3_out_keypath = s3keypath.S3_IMMIGRATION_DATA_DEMOGRAPHICS_PROCESSED_KEY_PATH
demographics_data_full_path = os.path.join(aws_s3_bucket_name,demographics_data_s3_keypath,demographics_data_file_name)
dataframe_demographics_data = spark.read.format("csv").option("delimiter", ",").option("header", "true").load(demographics_data_full_path)
dataframe_demographics_data.createOrReplaceTempView("demographics")
dim_demographics_data = spark.sql("""
                                SELECT  LOWER(TRIM(City)) as city, 
                                        State as state, 
                                        `Median Age` AS median_age, 
                                        `Male Population` AS male_population, 
                                        `Female Population` AS female_population, 
                                        `Total Population` AS total_population, 
                                        `Foreign-born` AS foreign_born, 
                                        `Average Household Size` AS average_household_size, 
                                        `State Code` AS state_code, 
                                        Race, 
                                        Count
                                FROM demographics
""")

state_codes_s3_key_path = s3keypath.S3_IMMIGRATION_DATA_CODES_STATE_PROCESSED_KEY_PATH
state_codes_data_full_path = os.path.join(aws_s3_bucket_name,state_codes_s3_key_path)
dataframe_state_codes = spark.read.parquet(state_codes_data_full_path)
dim_demographics_data = dim_demographics_data.join(dataframe_state_codes,["state_code"])
demographics_data_full_out_path = os.path.join(aws_s3_bucket_name,demographics_data_s3_out_keypath)
dim_demographics_data.write.mode("overwrite").parquet(demographics_data_full_out_path)



