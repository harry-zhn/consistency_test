import os

import boto3
import boto3.exceptions

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

bucket_name = "netapp-consistency-test"

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

def upload_file(s3_resource, bucket_name, key, file_path, metadata = None, acl = None):
    '''
    valid acl: # please make sure you have the permission to set acl on objects
        'pubic-read'
    '''
    extra_args = {}
    if metadata:
        extra_args["Metadata"] = metadata
    if acl:
        extra_args['ACL'] = acl
    s3_resource.Object(bucket_name, key).upload_file(file_path, ExtraArgs = extra_args)
    obj = s3_resource.Object(bucket_name, key)
    obj.download_file('/Users/harryzhang/git/consistency_test/test_data/download/test.txt')
    if obj.metadata == metadata:
        print("metadata match")

def list_objects(s3_client, bucket_name):
    return s3_client.list_objects_v2(Bucket="netapp-consistency-test")