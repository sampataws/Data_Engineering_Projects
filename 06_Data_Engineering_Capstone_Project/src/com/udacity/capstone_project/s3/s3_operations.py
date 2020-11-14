import boto3
import json
import os

from boto3.s3.transfer import TransferConfig
from com.udacity.capstone_project.s3 import s3_progress_percentage

from pathlib import Path

import configparser

de_config_path = Path(__file__).resolve().parents[1].parent.parent.parent
config = configparser.ConfigParser()
config.read('{}/config/de.cfg'.format(de_config_path))
BUCKET_NAME = config.get("AWS","BUCKET_NAME")
boto3.setup_default_session(profile_name='sampatawsadmin')

project_home = Path(__file__).resolve().parents[1].parent.parent.parent.parent
source_base_data_path = '{}/data'.format(project_home)

"""
NOTE: In order to leverage the pyboto3 please install -  pip install pyboto3

https://github.com/wavycloud/pyboto3

https://stackoverflow.com/questions/31555947/pycharm-intellisense-for-boto3

https://github.com/alliefitter/boto3_type_annotations/tree/master/boto3_type_annotations/boto3_type_annotations

"""
def s3_client():
    s3 = boto3.client('s3',region_name='us-west-1')
    """ :type : pyboto3.s3 """
    return s3

def s3_resource():
    s3 = boto3.resource('s3',region_name='us-west-1')
    return s3

def create_bucket(bucket_name):
    s3_client().create_bucket(Bucket=bucket_name,
        CreateBucketConfiguration={
            'LocationConstraint': 'us-west-1'
        })

def create_bucket_policy():
    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AddPerm",
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:*"],
                "Resource": ["arn:aws:s3:::simpplr-ingestion-workday-benchmarking/*"]
            }
        ]
    }

    policy_string = json.dumps(bucket_policy)

    return s3_client().put_bucket_policy(
        Bucket=BUCKET_NAME,
        Policy=policy_string
    )

def list_buckets():
    return s3_client().list_buckets()


def get_bucket_policy():
    return s3_client().get_bucket_policy(Bucket=BUCKET_NAME)


def get_bucket_encryption():
    return s3_client().get_bucket_encryption(Bucket=BUCKET_NAME)

def update_bucket_policy(bucket_name):
    bucket_policy = {
        'Version': '2012-10-17',
        'Statement': [
            {
                'Sid': 'AddPerm',
                'Effect': 'Allow',
                'Principal': '*',
                'Action': [
                    's3:DeleteObject',
                    's3:GetObject',
                    's3:PutObject'
                ],
                'Resource': 'arn:aws:s3:::' + bucket_name + '/*'
            }
        ]
    }

    policy_string = json.dumps(bucket_policy)

    return s3_client().put_bucket_policy(
        Bucket=bucket_name,
        Policy=policy_string
    )

def server_side_encrypt_bucket(bucket_name):
    return s3_client().put_bucket_encryption(
        Bucket=bucket_name,
        ServerSideEncryptionConfiguration={
            'Rules': [
                {
                    'ApplyServerSideEncryptionByDefault': {
                        'SSEAlgorithm': 'AES256'
                    }
                }
            ]
        }
    )

def delete_bucket(bucket_name):
    return s3_client().delete_bucket(Bucket=bucket_name)


def upload_small_file(bucket_name,file_name):
    file_path = os.path.dirname(__file__) + '/{}'.format(file_name)
    return s3_client().upload_file(file_path, bucket_name, file_name)


"""
https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-examples.html
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.upload_file

For ACL reference :
https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#CannedACL

For ContentType refernece :
https://docs.aws.amazon.com/macie/latest/userguide/macie-classify-objects-content-type.html

ALLOWED_DOWNLOAD_ARGS = ['VersionId', 'SSECustomerAlgorithm', 'SSECustomerKey', 'SSECustomerKeyMD5', 'RequestPayer']
ALLOWED_UPLOAD_ARGS (For ExtraArgs) = ['ACL', 'CacheControl', 'ContentDisposition', 'ContentEncoding', 'ContentLanguage',
                        'ContentType', 'Expires', 'GrantFullControl', 'GrantRead', 'GrantReadACP', 'GrantWriteACP',
                         'Metadata', 'RequestPayer', 'ServerSideEncryption', 'StorageClass', 'SSECustomerAlgorithm', 
                         'SSECustomerKey', 'SSECustomerKeyMD5', 'SSEKMSKeyId', 'Tagging', 'WebsiteRedirectLocation'
                        ]
"""




def upload_large_file(bucket_name, source_file_path,file_name,s3_folder_key_path):
    config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                            multipart_chunksize=1024 * 25, use_threads=True)
    file_path = source_file_path + '/' + file_name
    key_path = s3_folder_key_path +'/' + file_name
    s3_resource().meta.client.upload_file(file_path, bucket_name, key_path,
                                          ExtraArgs={'ACL': 'public-read', 'ContentType': 'text/json'},
                                          Config=config,
                                          Callback=s3_progress_percentage.S3DownloadProgressPercentage(file_path))

def version_bucket_files(bucket_name):
    s3_client().put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={
            'Status': 'Enabled'
        }
    )

def upload_a_new_version():
    file_path = os.path.dirname(__file__) + '/readme.txt'
    return s3_client().upload_file(file_path, BUCKET_NAME, 'readme.txt')

def put_lifecycle_policy():
    lifecycle_policy = {
        "Rules": [
            {
                "ID": "Move readme file to Glacier",
                "Prefix": "readme",
                "Status": "Enabled",
                "Transitions": [
                    {
                        "Date": "2019-01-01T00:00:00.000Z",
                        "StorageClass": "GLACIER"
                    }
                ]
            },
            {
                "Status": "Enabled",
                "Prefix": "",
                "NoncurrentVersionTransitions": [
                    {
                        "NoncurrentDays": 2,
                        "StorageClass": "GLACIER"
                    }
                ],
                "ID": "Move old versions to Glacier"
            }
        ]
    }

    s3_client().put_bucket_lifecycle_configuration(
        Bucket=BUCKET_NAME,
        LifecycleConfiguration=lifecycle_policy
    )



#if __name__ == '__main__':
    #upload_large_file(BUCKET_NAME,"/Users/sampatbudankayala/PycharmProjects/Data_Engineering_Projects/06_Data_Engineering_Capstone_Project/data/raw/demographics",'us-cities-demographics.csv','im-data/raw-staging/demographics/us-cities-demographics.csv')