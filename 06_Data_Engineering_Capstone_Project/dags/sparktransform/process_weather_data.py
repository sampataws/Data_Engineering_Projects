from pyspark.sql.types import StringType,IntegerType
import os

from  plugins.utils.constants import S3Buckets as s3bucket,S3AllImmigrationDataKeyPaths as s3keypath

aws_s3_bucket_name = "s3a://{}".format(s3bucket.UDACITY_DE_PROJECT_BUCKET_NAME)
weather_data_file_name = "GlobalLandTemperaturesByCity.csv"
weather_data_s3_keypath = s3keypath.S3_IMMIGRATION_DATA_WEATHER_STAGING_KEY_PATH
weather_data_s3_out_keypath = s3keypath.S3_IMMIGRATION_DATA_WEATHER_PROCESSED_KEY_PATH


weather_data_full_path = os.path.join(aws_s3_bucket_name,weather_data_s3_keypath,weather_data_file_name)

""" Note that we don’t need to import and create the spark session as it’s already provided as spark by the 
    spark shell while creating spark session via Livy ."""

dataframe_weather_data = spark.read.format("csv").option("delimiter", ",").option("encoding", "UTF-8").option("header", "true").load(weather_data_full_path)

# Process Data only for United States , so applying filter on weather dataFrame
"""
Sample Weather data:
dt,AverageTemperature,AverageTemperatureUncertainty,City,Country,Latitude,Longitude
1743-11-01,6.068,1.7369999999999999,Århus,Denmark,57.05N,10.33E
"""
dataframe_weather_data_for_us = dataframe_weather_data.filter("Country == 'United States'")

# Coverting the date column to DateTime object for "dt"

from pyspark.sql.functions import unix_timestamp, from_unixtime,lit,to_date

dataframe_weather_data_for_us_with_date_conv = dataframe_weather_data_for_us.select(
    'AverageTemperature','AverageTemperatureUncertainty','City',
    'Country','Latitude','Longitude',
    from_unixtime(unix_timestamp('dt','yyyy-MM-dd')).alias('date')
)

dataframe_weather_data_for_us_with_date_conv_prior_1950 = dataframe_weather_data_for_us_with_date_conv_prior_1950 = dataframe_weather_data_for_us_with_date_conv.filter("date > date'1950-01-01'")

from pyspark.sql.functions import year, month, dayofmonth, udf

udf_conver_to_lower = udf(lambda clmn_val:str(clmn_val).lower(),StringType())


dataframe_weather_us = dataframe_weather_data_for_us_with_date_conv_prior_1950.select(
    dataframe_weather_data_for_us_with_date_conv.date,
    year("date").alias("Year"),
    month("date").alias("Month"),
    dayofmonth("date").alias("DayOfMonth"),
    dataframe_weather_data_for_us_with_date_conv_prior_1950.AverageTemperature,
    dataframe_weather_data_for_us_with_date_conv_prior_1950.AverageTemperatureUncertainty,
    udf_conver_to_lower("City").alias("city"),
    dataframe_weather_data_for_us_with_date_conv_prior_1950.Latitude,
    dataframe_weather_data_for_us_with_date_conv_prior_1950.Longitude
)

dataframe_weather_us.createOrReplaceTempView("weather")

dim_weather_us = spark.sql("""
SELECT
    DISTINCT date,Year,Month, city,
    AVG(AverageTemperature) OVER (PARTITION BY date, City) AS average_temperature, 
    AVG(AverageTemperatureUncertainty)  OVER (PARTITION BY date, City) AS average_termperature_uncertainty
FROM weather
""")


dim_weather_us.write.partitionBy("Year","Month","city").mode("overwrite").parquet("{}{}".format(aws_s3_bucket_name,weather_data_s3_out_keypath))












