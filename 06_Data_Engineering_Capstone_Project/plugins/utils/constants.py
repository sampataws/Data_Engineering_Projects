class S3Buckets:
    UDACITY_DE_PROJECT_BUCKET_NAME = 'dataengineering-capstone-project-bucket'

class S3AllImmigrationDataKeyPaths:
    S3_IMMIGRATION_DATA_KEY_PATH = '/im-data/'
    S3_IMMIGRATION_DATA_STAGING_KEY_PATH = '/im-data/raw-staging/'
    S3_IMMIGRATION_DATA_AIRPORT_CODE_STAGING_KEY_PATH = '/im-data/raw-staging/airportcode/'
    S3_IMMIGRATION_DATA_LABEL_CODE_STAGING_KEY_PATH = '/im-data/raw-staging/codes/'
    S3_IMMIGRATION_DATA_WEATHER_STAGING_KEY_PATH = '/im-data/raw-staging/globaltemperatures/'
    S3_IMMIGRATION_DATA_DEMOGRAPHICS_STAGING_KEY_PATH = '/im-data/raw-staging/demographics/'
    S3_IMMIGRATION_DATA_I94_STAGING_KEY_PATH = '/im-data/raw-staging/i94-immigration-data/18-83510-I94-Data-2016/'

    S3_IMMIGRATION_DATA_PROCESS_KEY_PATH = '/im-data/processed-data/'
    S3_IMMIGRATION_DATA_WEATHER_PROCESSED_KEY_PATH = '/im-data/processed-data/dim-weather/'
    S3_IMMIGRATION_DATA_AIRPORT_PROCESSED_KEY_PATH = '/im-data/processed-data/dim-airport/'
    S3_IMMIGRATION_DATA_DEMOGRAPHICS_PROCESSED_KEY_PATH = '/im-data/processed-data/dim-demographics/'



    S3_IMMIGRATION_DATA_CODES_AIRPORT_PROCESSED_KEY_PATH = '/im-data/processed-data/codes/airport/'
    S3_IMMIGRATION_DATA_CODES_COUNTRY_PROCESSED_KEY_PATH = '/im-data/processed-data/codes/country/'
    S3_IMMIGRATION_DATA_CODES_STATE_PROCESSED_KEY_PATH = '/im-data/processed-data/codes/state/'
    S3_IMMIGRATION_DATA_CODES_MODE_PROCESSED_KEY_PATH = '/im-data/processed-data/codes/mode/'
    S3_IMMIGRATION_DATA_CODES_VISA_PROCESSED_KEY_PATH = '/im-data/processed-data/codes/visa/'

    S3_IMMIGRATION_FACT_TABLE_PATH='/im-data/processed-data/tables/fact/immigration'



