import os
from com.udacity.capstone_project.s3 import s3_operations
from pathlib import Path
import configparser

de_config_path = Path(__file__).resolve().parents[1].parent.parent.parent
project_home = Path(__file__).resolve().parents[1].parent.parent.parent.parent
source_base_data_path = '{}/data'.format(project_home)

config = configparser.ConfigParser()
config.read('{}/config/de.cfg'.format(de_config_path))
BUCKET_NAME = config.get("AWS","BUCKET_NAME")


def uploadImmigDatatoS3(s3_key_path,bucket_name=BUCKET_NAME):
    source_immigration_data_path = "{}/raw/i94_immigration_data/18-83510-I94-Data-2016".format(source_base_data_path)
    files = [source_base_data_path + f for f in os.listdir("{}".format(source_immigration_data_path))]

    for file in files:
        file_head_tail = os.path.split(file)
        source_file_path = file_head_tail[0]
        file_name = file_head_tail[1]
        s3_operations.upload_large_file(bucket_name,source_file_path,file_name,s3_key_path)

def uploadDemographicsDataToS3(s3_key_path,bucket_name=BUCKET_NAME):
    source_demographic_file_path = "{}/raw/demographics/us-cities-demographics.csv".format(source_base_data_path)
    file_head_tail = os.path.split(source_demographic_file_path)
    source_file_path = file_head_tail[0]
    file_name = file_head_tail[1]
    s3_operations.upload_large_file(bucket_name, source_file_path, file_name, s3_key_path)

def uploadGlobalTemperaturesDataToS3(s3_key_path,bucket_name=BUCKET_NAME):
    source_temperature_file_path = "{}/raw/globaltemperatures/GlobalLandTemperaturesByCity.csv".format(source_base_data_path)
    file_head_tail = os.path.split(source_temperature_file_path)
    source_file_path = file_head_tail[0]
    file_name = file_head_tail[1]
    s3_operations.upload_large_file(bucket_name, source_file_path, file_name, s3_key_path)

def uploadAirportCodesDataToS3(s3_key_path,bucket_name=BUCKET_NAME):
    source_airport_codes_file_path = "{}/raw/airportcode/airport-codes_csv.csv".format(source_base_data_path)
    file_head_tail = os.path.split(source_airport_codes_file_path)
    source_file_path = file_head_tail[0]
    file_name = file_head_tail[1]
    s3_operations.upload_large_file(bucket_name, source_file_path, file_name, s3_key_path)

def uploadCodesDataToS3(s3_key_path,bucket_name=BUCKET_NAME):
    source_codes_data_path = "{}/raw/codes/".format(source_base_data_path)
    files = [source_codes_data_path + f for f in os.listdir("{}".format(source_codes_data_path))]

    for file in files:
        file_head_tail = os.path.split(file)
        source_file_path = file_head_tail[0]
        file_name = file_head_tail[1]
        s3_operations.upload_large_file(bucket_name, source_file_path, file_name, s3_key_path)


uploadImmigDatatoS3("im-data/raw-staging/i94-immigration-data/18-83510-I94-Data-2016")
uploadDemographicsDataToS3("im-data/raw-staging/demographics")
uploadGlobalTemperaturesDataToS3("im-data/raw-staging/globaltemperatures")
uploadAirportCodesDataToS3("im-data/raw-staging/airportcode")
uploadCodesDataToS3("im-data/raw-staging/codes")






