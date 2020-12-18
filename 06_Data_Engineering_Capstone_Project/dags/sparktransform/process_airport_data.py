from pyspark.sql.types import StringType,IntegerType,DoubleType
from pyspark.sql.functions import udf

import os

from  plugins.utils.constants import S3Buckets as s3bucket,S3AllImmigrationDataKeyPaths as s3keypath

aws_s3_bucket_name = "s3a://{}".format(s3bucket.UDACITY_DE_PROJECT_BUCKET_NAME)
airport_data_file_name = "airport-codes_csv.csv"
airport_data_s3_keypath = s3keypath.S3_IMMIGRATION_DATA_AIRPORT_CODE_STAGING_KEY_PATH
airport_data_s3_out_keypath = s3keypath.S3_IMMIGRATION_DATA_WEATHER_PROCESSED_KEY_PATH


def getLatitude(coordinates):
    latitude = coordinates.strip().split(",",1)
    return float(latitude[1])

def getLongitude(coordinates):
    longitude = coordinates.strip().split(",",1)
    return float(longitude[1])

def getState(isoegion):
    state = None
    if(isoegion is not None):
        lst = isoegion
        lst = isoegion.strip().split("-",1)
        state = lst[1]
    return state

udf_get_latitude = udf(lambda lat: getLatitude(lat), DoubleType())
udf_get_longitude = udf(lambda lgt: getLongitude(lgt), DoubleType())
udf_get_state = udf(lambda x:getState(x),StringType())


airport_data_full_path = os.path.join(aws_s3_bucket_name,airport_data_s3_keypath,airport_data_file_name)

""" Note that we don’t need to import and create the spark session as it’s already provided as spark by the 
    spark shell while creating spark session via Livy ."""


"""from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()"""

dataframe_airport_data = spark.read.format("csv").option("delimiter", ",").option("header", "true").option("encoding", "UTF-8").load(airport_data_full_path)

"""
Sample Airport Data
ident,type,name,elevation_ft,continent,iso_country,iso_region,municipality,gps_code,iata_code,local_code,coordinates
00A,heliport,Total Rf Heliport,11,NA,US,US-PA,Bensalem,00A,,00A,"-74.93360137939453, 40.07080078125"
"""

dataframe_airport_data.createOrReplaceTempView("airports")

dataframe_airport_data_filtered = spark.sql("""
SELECT *
FROM airports
WHERE iso_country IS NOT NULL
AND UPPER(TRIM(iso_country)) LIKE 'US' 
AND LOWER(TRIM(type)) NOT IN ('closed', 'heliport', 'seaplane_base', 'balloonport')
AND municipality IS NOT NULL
AND LENGTH(iso_region) = 5
AND iata_code IS NOT NULL
""")

dataframe_airport_data_transfomed_after_filter = dataframe_airport_data_filtered.withColumn("latitude",udf_get_latitude("coordinates")).withColumn("longitude",udf_get_longitude("coordinates")).withColumn("state",udf_get_state("iso_region"))
dataframe_aiport_with_column_rename = dataframe_airport_data_transfomed_after_filter.withColumnRenamed("municipality","city").withColumnRenamed("iata_code","airport_code")
dataframe_airport = dataframe_aiport_with_column_rename.select(["ident","type","name","city","state","gps_code","airport_code","local_code","latitude","longitude"])

airport_codes_s3_key_path = s3keypath.S3_IMMIGRATION_DATA_AIRPORT_PROCESSED_KEY_PATH
full_s3_join_path_for_airport_codes = os.path.join(aws_s3_bucket_name,airport_codes_s3_key_path)
dataframe_us_ports_data = spark.read.parquet(full_s3_join_path_for_airport_codes)

dim_airport_immigration =  dataframe_airport.join(dataframe_us_ports_data,["airport_code"])
airport_data_full_out_path = os.path.join(aws_s3_bucket_name,airport_data_s3_out_keypath)
dim_airport_immigration.write.mode("overwrite").parquet(airport_data_full_out_path)
