#!python3
import os
import datetime
from sys import prefix

from botocore.retries import bucket

import common


'''
Ideas:
    1. to PUT 200 objects into the bucket first.
        1.1 the object id with a surfix of integer.
        2.2 put all the keys into a list.
    2. DELETE object
        2.1  add two more objects into the bucket, delete one object per iteration by following sequence of the list.
        2.2 list objects remotely, and sort keys retrieved. all the keys should consecutive. everytime, number objects should be the same or increase by 1.
'''
def validate_object_keys(response):
    contents = response['Contents']
    object_keys = map(lambda obj: int(obj['Key'][len(common.prefix_for_delete_and_list) + 1:]), contents)
    object_keys = sorted(object_keys)
    size_object_keys = len(object_keys)
    if size_object_keys > 1:
        for index in range(1, size_object_keys):
            if object_keys[index] - object_keys[index-1] != 1:
                print("object keys in the list is not consecutive!")
                return False
    return True

def list_test(s3_client, repeat = 200):
    '''
    to use list_objects_v2 with boto3.client
    '''
    local_work_dir = os.path.dirname(os.path.abspath(__file__))
    download_dir = os.path.join(local_work_dir, "test_data/download")
    common.prepare_local_folder(download_folder=download_dir)
    current_time = datetime.datetime.now(tz = datetime.timezone.utc)
    current_date = current_time.date()
    time_stamp = current_time.time()

    part_of_filename = f'{current_date.year:04}-{current_date.month:02}-{current_date.day:02}_{time_stamp.hour:02}-{time_stamp.minute:02}-{time_stamp.second:02}'
    report_filename = os.path.join(local_work_dir, "test_data", f'{part_of_filename}_list_report.txt')
    
    with open(report_filename, "x") as report_file:
        report_file.write(f"read test for list objects at {current_time} \n")
        report_file.write(f'trying to repeat {repeat} times\n')
        for _ in range(repeat):
            response = s3_client.list_objects_v2(Bucket = common.bucket_name, Prefix=common.prefix_for_delete_and_list)
            contents = response['Contents']
            if not validate_object_keys(response):
                print(contents, file=report_file)

        end_time = datetime.datetime.now(tz = datetime.timezone.utc)
        deltatime = end_time - current_time
        report_file.write(f'end time: {end_time}, total used: {deltatime} \n')
        report_file.write("=====end of report==")


def write_and_list(s3_resource, s3_client, repeat = 200):
    '''
    to add two files and delete one files
    '''
    local_work_dir = os.path.dirname(os.path.abspath(__file__))
    upload_dir = os.path.join(local_work_dir, "test_data/upload")
    download_dir = os.path.join(local_work_dir, "test_data/download")
    common.prepare_local_folder(upload_folder=upload_dir, download_folder=download_dir)

    current_time = datetime.datetime.now(tz = datetime.timezone.utc)
    current_date = current_time.date()
    time_stamp = current_time.time()

    part_of_filename = f'{current_date.year:04}-{current_date.month:02}-{current_date.day:02}_{time_stamp.hour:02}-{time_stamp.minute:02}-{time_stamp.second:02}'
    report_filename = os.path.join(local_work_dir, "test_data", f'{part_of_filename}_list_with_write_and_delete_report.txt')

    key_boundary1 = 200
    # to generate 200 object first
    for key_id in range(key_boundary1):
        object_key = f'{common.prefix_for_delete_and_list}/{key_id:03}'
        short_filename = f'{key_id}_{part_of_filename}'
        upload_file = os.path.join(upload_dir, short_filename)

        common.upload_object_with_random_data(s3_resource, object_key, upload_file)
    with open(report_filename, 'x') as report_file:
        for step in range(repeat):
            key_id = step + key_boundary1
            object_key = f'{common.prefix_for_delete_and_list}/{key_id:03}'
            short_filename = f'{key_id}_{part_of_filename}'
            upload_file = os.path.join(upload_dir, short_filename)
            common.upload_object_with_random_data(s3_resource, object_key, upload_file)

            key_id = step
            object_key = f'{common.prefix_for_delete_and_list}/{key_id:03}'
            s3_object = s3_resource.Object(common.bucket_name, object_key)
            s3_object.delete()
            response = s3_client.list_objects_v2(Bucket = common.bucket_name, Prefix=common.prefix_for_delete_and_list)
            contents = response['Contents']
            if not validate_object_keys(response):
                print(contents, file=report_file)

        end_time = datetime.datetime.now(tz = datetime.timezone.utc)
        deltatime = end_time - current_time
        report_file.write(f'end time: {end_time}, total used: {deltatime} \n')
        report_file.write("=====end of report==")

def test_with_list(credential_tag, endpoint_url = None, verify_cert = True):
    if not common.read_aws_credential(credential_tag):
        raise Exception("cannot find credential")
    
    aws_access_key = os.environ[common.key_tag]
    aws_secret_access = os.environ[common.secret_key_tag]
    s3_client = common.get_s3_client(aws_access_key, aws_secret_access, endpoint = endpoint_url, verify_ssl_cert = verify_cert)
    repeat = 300
    list_test(s3_client, repeat= repeat)

def test_with_write_and_list(credential_tag, endpoint_url = None, verify_cert = True):
    if not common.read_aws_credential(credential_tag):
        raise Exception("cannot find credential")
    
    aws_access_key = os.environ[common.key_tag]
    aws_secret_access = os.environ[common.secret_key_tag]
    s3_resource = common.get_s3_resource(aws_access_key, aws_secret_access, endpoint = endpoint_url, verify_ssl_cert = verify_cert)
    s3_client = common.get_s3_client(aws_access_key, aws_secret_access, endpoint = endpoint_url, verify_ssl_cert = verify_cert)
    repeat = 200
    write_and_list(s3_resource, s3_client, repeat= repeat)
