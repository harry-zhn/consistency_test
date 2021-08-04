import os
import random
import datetime
import uuid
import hashlib

import boto3
import botocore

import common

object_properties = [
    #'delete',
    'e_tag',
    #'get',
    #'get_available_subresources',
    #'initiate_multipart_upload', # this is static function
    'key',
    'last_modified',
    #'load',
    'meta',
    'owner',
    #'put', #to upload an object
    #'restore_object',
    'size',
    'storage_class',
    'wait_until_exists',
    'wait_until_not_exists'
]

bucket_name = "zharry-consistency-level-test"
object_key_for_overwrite = "overwrite_this"
object_key_read_after_delete = "read_after_delete"
prefix_for_delete_and_list = "delete_and_list"

key_tag = "aws_access_key_id"
secret_key_tag = "aws_secret_access_key"

def read_aws_credential(tag = None):
    file_path = "~/.aws/credentials"
    file_path = os.path.expanduser(file_path)
    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)
    if not tag:
        tag = "[default]"
    else:
        tag = '[' + tag + ']\n'

    found_tag = False

    key_id = ""
    secret_access_key = ""
    with open(file_path) as credentials:
        for line in credentials:
            if line == tag:
                found_tag = True
                continue
            if found_tag and line.find(key_tag) == 0:
                parts = line.split("=")
                if parts and len(parts) > 1:
                    key_id = parts[1].strip()
                    os.environ[key_tag] = key_id
            if found_tag and line.find(secret_key_tag) == 0:
                parts = line.split("=")
                if parts and len(parts) > 1:
                    secret_access_key = parts[1].strip()
                    os.environ[secret_key_tag] = secret_access_key
            if key_id and secret_access_key:
                break
    return len(key_id) > 0 and len(secret_access_key) > 0


def get_s3_client(access_key, secret_access_key, endpoint = None, verify_ssl_cert = False):
    # https://boto3.amazonaws.com/v1/documentation/api/latest/_modules/boto3/session.html#Session.client
    s3_client = boto3.client("s3", aws_access_key_id = access_key, aws_secret_access_key = secret_access_key, endpoint_url = endpoint, verify = verify_ssl_cert )
    return s3_client

def get_s3_resource(access_key, secret_access_key, endpoint = None, verify_ssl_cert = False, use_ssl = True):
    # https://boto3.amazonaws.com/v1/documentation/api/latest/_modules/boto3/session.html#Session.client
    s3_resource = boto3.resource("s3", verify = verify_ssl_cert, endpoint_url = endpoint, aws_access_key_id = access_key, aws_secret_access_key = secret_access_key)
    return s3_resource

def get_bucket(s3_resource, bucket_name):
    bucket = s3_resource.Bucket(bucket_name)
    return bucket

def prepare_local_folder(upload_folder = None, download_folder = None):
    if upload_folder: os.makedirs(upload_folder, exist_ok = True)
    if download_folder: os.makedirs(download_folder, exist_ok = True)

def list_objects(s3_client, bucket_name):
    return s3_client.list_objects_v2(Bucket="netapp-consistency-test")

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
            chunk = f.read(8192)
            while chunk:
                file_hash.update(chunk)
                chunk = f.read(8192)

        file_hash_result = str.format('"{}"',file_hash.hexdigest())
    return file_hash_result

def empty_bucket(s3_resource, bucket_name):
    bucket = get_bucket(s3_resource, bucket_name)
    for obj in bucket.objects.all():
        obj.delete()
    print(f"s3 bucket:[{bucket_name}] should be empty now")
    for obj in bucket.objects.all():
        print(obj.key)

def upload_object_with_random_data(s3_resource, object_key, upload_filepath):
    int_range = 2 ** 10
    with open(upload_filepath, 'x') as source:
        source.write(f"overwrite test at {datetime.datetime.now(tz = datetime.timezone.utc)}")
        loop_count = random.randrange(10, 30)
        for i in range(loop_count):
            num = random.randint(-int_range, int_range)
            source.write(str.format("{} \n",num))
        
        metadata  = {'prop1': str(random.randint(-int_range, int_range)),
                    'prop2': str(random.randint(-int_range, int_range)),
                    'uuid': str(uuid.uuid4())
                    }
        extra_args = {"Metadata": metadata}
        s3_resource.Object(common.bucket_name, object_key).upload_file(upload_filepath, ExtraArgs = extra_args )
        return metadata



