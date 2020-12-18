import os

from  plugins.utils.constants import S3Buckets as s3bucket,S3AllImmigrationDataKeyPaths as s3keypath

aws_s3_bucket_name = "s3a://{}".format(s3bucket.UDACITY_DE_PROJECT_BUCKET_NAME)
demographics_data_file_name = "i94_{0}_sub.sas7bdat"
demographics_data_s3_keypath = s3keypath.S3_IMMIGRATION_DATA_I94_STAGING_KEY_PATH

immigration_data = "{}{}{}".format(aws_s3_bucket_name,demographics_data_s3_keypath,demographics_data_file_name)
immigration_data = immigration_data.format(fileYearMonth)
dataframe_immigration_data = spark.read.format("com.github.saurfang.sas.spark").load(immigration_data, forceLowercaseNames=True, inferLong=True)

import pyspark.sql.functions as F
import datetime as dte
udf_parse_arrival_dt = F.udf(lambda x: (dte.datetime(1960, 1, 1).date() + dte.timedelta(x)).isoformat() if x else None)

# Adding arrival and departure date
dataframe_immigration_data = dataframe_immigration_data.withColumn("arrival_date",udf_parse_arrival_dt(dataframe_immigration_data.arrdate))
dataframe_immigration_data = dataframe_immigration_data.withColumn("departure_date",udf_parse_arrival_dt(dataframe_immigration_data.depdate))

i94model_data = [{"code": '1', "transportation": "Air"},
        {"code": '2', "transportation": "Sea"},
        {"code": '3', "transportation": "Land"}
        ]

dataframe_i94mode = spark.createDataFrame(i94model_data)
dataframe_state = spark.read.parquet(aws_s3_bucket_name+s3keypath.S3_IMMIGRATION_DATA_CODES_STATE_PROCESSED_KEY_PATH)
dataframe_country = spark.read.parquet(aws_s3_bucket_name+s3keypath.S3_IMMIGRATION_DATA_CODES_COUNTRY_PROCESSED_KEY_PATH)
dataframe_i94visa = spark.read.parquet(aws_s3_bucket_name+s3keypath.S3_IMMIGRATION_DATA_CODES_VISA_PROCESSED_KEY_PATH)
dataframe_airportcode = spark.read.parquet(aws_s3_bucket_name+s3keypath.S3_IMMIGRATION_DATA_AIRPORT_PROCESSED_KEY_PATH)


dataframe_immigration_data = dataframe_immigration_data.withColumnRenamed("i94yr","year").withColumnRenamed("i94mon","month").withColumnRenamed("i94port","port_of_entry").withColumnRenamed("i94bir","age")

dataframe_immigration_data = dataframe_immigration_data.join(F.broadcast(dataframe_i94mode),["i94mode"])
dataframe_immigration_data = dataframe_immigration_data.filter("mode == 'Air'")


dataframe_immigration_data.createOrReplaceTempView("immigration_table")
"""
    Next, we replace the data in the I94VISA columns The three categories are:

    1 = Business
    2 = Pleasure
    3 = Student

"""

spark.sql("""
            SELECT *, 
                    CASE 
                        WHEN i94visa = 1.0 THEN 'Business' 
                        WHEN i94visa = 2.0 THEN 'Pleasure'
                        WHEN i94visa = 3.0 THEN 'Student'
                    ELSE 'N/A' END AS visa 
                FROM immigration_table
            """
          ).createOrReplaceTempView("immigration_table")

"""
Removing Invalid records where departure date is greater than arrival date
"""

spark.sql("""
                SELECT *
                FROM immigration_table
                WHERE departure_date >= arrival_date
           """
          ).createOrReplaceTempView("immigration_table")

dataframe_country.createOrReplaceTempView("countryCodes")

dataframe_airportcode.createOrReplaceTempView("i94portCodes")

"""
Dropping Invalid country codes
"""
spark.sql("""
                SELECT im.*, cc.country AS citizenship_country
                FROM immigration_table im
                INNER JOIN countryCodes cc
                ON im.i94cit = cc.code
           """
          ).createOrReplaceTempView("immigration_table")

spark.sql("""
                SELECT im.*, cc.country AS residence_country
                FROM immigration_table im
                INNER JOIN countryCodes cc
                ON im.i94res = cc.code
           """
          ).createOrReplaceTempView("immigration_table")

immigration_fact_table = spark.sql("""
                                        SELECT 
                                                cast(cicid as int), 
                                                citizenship_country,
                                                residence_country,
                                                TRIM(UPPER (entry_port)) AS city,
                                                TRIM(UPPER (entry_port_state)) AS state,
                                                CAST(biryear as int)  AS birth_year,
                                                arrival_date,
                                                departure_date,
                                                cast(age as int),
                                                visa,
                                                visatype AS detailed_visa_type
                                        FROM immig_table
                                    """
                                   )


immigration_fact_table.write.mode("overwrite").parquet("{}{}".format(s3bucket,s3keypath.S3_IMMIGRATION_FACT_TABLE_PATH ))






