# python 3.8+
#please set BUCKET and CREDENTIAL before using the script
import os
import hashlib
import boto3
from botocore.retries import bucket

def get_s3_bucket():
    default_val = ''
    BUCKET = os.getenv('S3_BUCKET', default_val)
    CREDENTIAL = os.getenv('AWS_CREDENTIAL', default_val)

    session = boto3.Session(profile_name = CREDENTIAL)
    s3 = session.resource('s3')
    bucket = s3.Bucket(BUCKET)
    return bucket

def list_s3_objects(bucket):
    for obj in bucket.objects.all():
        print(obj.key)

def get_MD5(full_file_path):
    '''
    This is to generate ETag of upload
    '''
    file_hash_result = ""
    full_file_path = os.path.expanduser(full_file_path)
    full_file_path = os.path.expandvars(full_file_path)
    if os.path.isfile(full_file_path):
        file_hash = hashlib.md5()
        with open(full_file_path, "rb") as f:
            while chunk := f.read(8192):
                file_hash.update(chunk)

        file_hash_result = file_hash.hexdigest()
    return file_hash_result