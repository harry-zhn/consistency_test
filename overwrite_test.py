'''
To test consistency level regarding read-after-write
    PutObject
    GetObject
    HeadObject
    CopyObject
Important data to verify
    object key,
        [size, last_modified, metadata]
        acl
0. create a new object with meta data
    0.0 keep local record of object size, metadata, and file content
    0.1 retrieve object for [size, last_modified, metadata] and download file itself
1. keep updating the object for 200 times with content and meta data
    1.0 keep local record of object size, metadata, and file content
    1.1 retrieve object for [size, last_modified, metadata] and download file itself

local record            | remote record   | notes
=====================================================================================
0.0                     | 0.1             | expect record[0.0] == record[0.1] 
-------------------------------------------------------------------------------------
1.0                     | 1.1             | for 200 times, 
                        |                 |     expect record[1.1] == record[1.1]
'''

import os
import datetime
import random

from botocore.retries import bucket
import common

def test_object_tagging(s3_resource, s3_client, repeat = 200):
    local_work_dir = os.path.dirname(os.path.abspath(__file__))
    upload_dir = os.path.join(local_work_dir, "test_data/upload")
    download_dir = os.path.join(local_work_dir, "test_data/download")
    common.prepare_local_folder(upload_folder=upload_dir, download_folder=download_dir)

    object_key = common.object_key_for_tagging
    current_time = datetime.datetime.now(tz = datetime.timezone.utc)
    current_date = current_time.date()
    time_stamp = current_time.time()

    part_of_filename = f'{current_date.year:04}-{current_date.month:02}-{current_date.day:02}_{time_stamp.hour:02}-{time_stamp.minute:02}-{time_stamp.second:02}'
    # make sure the object exists
    short_filename = f'tagging_test_{part_of_filename}'
    upload_file = os.path.join(upload_dir, short_filename)

    metadata = common.upload_object_with_random_data(s3_resource, object_key, upload_file)
    original_size = os.stat(upload_file).st_size


    prev_time = current_time
    report_filename = os.path.join(local_work_dir, "test_data", f'{part_of_filename}_report.txt')

    with open(report_filename, "x") as report_file:
        report_file.write(f"object tagging test on key {object_key} at {current_time} \n")
        report_file.write(f'trying to repeat {repeat} times\n')
        tag_lookup = {
            'Key1': 0, 
            'Key2': 0, 
            'Key3': 0, 
            'Key4': 0, 
            'Key5': 0, 
        }
        int_range = 10
        for step in range(repeat):
            tag_list = []
            for tag in tag_lookup:
                val = random.randrange(1, int_range)
                tag_lookup[tag] += val
                tag_list.append({'Key':tag, 'Value': str(tag_lookup[tag])})
            s3_client.put_object_tagging(Bucket = common.bucket_name, Key = object_key, Tagging = {'TagSet': tag_list})
            response = s3_client.get_object_tagging(Bucket = common.bucket_name, Key = object_key)
            tag_list_returned = response['TagSet']
            if tag_list != tag_list_returned:
                #report error
                if len(tag_list) != len(tag_list_returned):
                    print(f"at step {step}, expect to have {len(tag_list)}, Got {len(tag_list_returned)}", file=report_file)
                else:
                    size = len(tag_list)
                    for index in range(size):
                        tag = tag_list_returned[index]
                        if int(tag['Value']) != tag_lookup[tag['Key']]:
                            print(f"At step {step}, Expect to have value {tag_lookup[tag['Key']]}, got {tag['Value']}", file=report_file)

        end_time = datetime.datetime.now(tz = datetime.timezone.utc)
        deltatime = end_time - current_time
        report_file.write(f'end time: {end_time}, total used: {deltatime} \n')
        report_file.write("=====end of report==")

def copy_object(s3_resource, s3_client,  repeat = 200):
    '''
    1. create 200 objects.
    2. overwrite test with CopyObject
    '''
    local_work_dir = os.path.dirname(os.path.abspath(__file__))
    upload_dir = os.path.join(local_work_dir, "test_data/upload")
    download_dir = os.path.join(local_work_dir, "test_data/download")
    common.prepare_local_folder(upload_folder=upload_dir, download_folder=download_dir)

    object_key = common.object_key_for_overwrite
    current_time = datetime.datetime.now(tz = datetime.timezone.utc)
    current_date = current_time.date()
    time_stamp = current_time.time()

    prev_time = current_time
    part_of_filename = f'{current_date.year:04}-{current_date.month:02}-{current_date.day:02}_{time_stamp.hour:02}-{time_stamp.minute:02}-{time_stamp.second:02}'
    report_filename = os.path.join(local_work_dir, "test_data", f'{part_of_filename}_report.txt')

    object_key_prefix = "to_be_copied"
    with open(report_filename, "x") as report_file:
        report_file.write(f"test on key {object_key} at {current_time} \n")
        report_file.write(f'trying to repeat {repeat} times\n')
        objects = []
        for step in range(repeat):
            short_filename = str.format('{0}_{1}', step, part_of_filename)
            upload_file = os.path.join(upload_dir, short_filename)
            object_key = f'{object_key_prefix}-{step:03}'

            metadata = common.upload_object_with_random_data(s3_resource, object_key, upload_file)
            original_size = os.stat(upload_file).st_size
            objects.append(( object_key, metadata, original_size, upload_file))

        object_key = common.object_key_for_overwrite
        copy_source = {'Bucket': common.bucket_name, 'Key':''}
        for source_object_key, metadata, original_size, upload_file in objects:
            copy_source['Key'] = source_object_key
            response = s3_client.copy_object(Bucket = common.bucket_name, Key = object_key, CopySource = copy_source)
            e_tag_copy_result = response['CopyObjectResult']['ETag']

            # to GetObject for verification
            download_file = os.path.join(download_dir, short_filename)
            s3_obj = s3_resource.Object(common.bucket_name, object_key)
            s3_obj.download_file(download_file)
            download_size = os.stat(download_file).st_size
            
            #validation
            if s3_obj.metadata != metadata:
                print("metadata mismatch", file = report_file)
                print("metadata on download", s3_obj.metadata, file = report_file)
                print("expected metadata", metadata, file = report_file)
            if original_size != download_size:
                print("file size mismatch, original size: ", original_size, "download size: ", download_size , file = report_file)
            if original_size != s3_obj.content_length:
                print("file size mismatch, original size: ", original_size, "content_length: ", s3_obj.content_length , file = report_file)
            e_tag_upload = common.get_MD5(upload_file)
            e_tag_download = common.get_MD5(download_file)
            if e_tag_copy_result != s3_obj.e_tag:
                print("ETag from CopyObjectResult doesn't match with the latest version of Object. CopyResult:", e_tag_copy_result, "ETag on Object:", s3_obj.e_tag, file = report_file)
            if e_tag_download != e_tag_upload:
                print("file content md5 mismatch. uploaded file, ", e_tag_upload, " downloaded file, ", e_tag_download, file = report_file)
            if e_tag_upload != s3_obj.e_tag:
                print("e_tag is wrong as meta data, ", s3_obj.e_tag, "original file, ", e_tag_upload, file = report_file)
        end_time = datetime.datetime.now(tz = datetime.timezone.utc)
        deltatime = end_time - current_time
        report_file.write(f'end time: {end_time}, total used: {deltatime} \n')
        report_file.write("=====end of report==")

def test(s3_resource, repeat = 200):
    local_work_dir = os.path.dirname(os.path.abspath(__file__))
    upload_dir = os.path.join(local_work_dir, "test_data/upload")
    download_dir = os.path.join(local_work_dir, "test_data/download")
    common.prepare_local_folder(upload_folder=upload_dir, download_folder=download_dir)

    object_key = common.object_key_for_overwrite
    current_time = datetime.datetime.now(tz = datetime.timezone.utc)
    current_date = current_time.date()
    time_stamp = current_time.time()

    prev_time = current_time
    part_of_filename = f'{current_date.year:04}-{current_date.month:02}-{current_date.day:02}_{time_stamp.hour:02}-{time_stamp.minute:02}-{time_stamp.second:02}'
    report_filename = os.path.join(local_work_dir, "test_data", f'{part_of_filename}_report.txt')
    with open(report_filename, "x") as report_file:
        report_file.write(f"test on key {object_key} at {current_time} \n")
        report_file.write(f'trying to repeat {repeat} times\n')
        for step in range(repeat):
            short_filename = str.format('{0}_{1}', step, part_of_filename)
            upload_file = os.path.join(upload_dir, short_filename)

            metadata = common.upload_object_with_random_data(s3_resource, object_key, upload_file)
            original_size = os.stat(upload_file).st_size
            
            #to GetObject for verification
            download_file = os.path.join(download_dir, short_filename)
            s3_obj = s3_resource.Object(common.bucket_name, object_key)
            s3_obj.download_file(download_file)
            download_size = os.stat(download_file).st_size
            
            #validation
            if s3_obj.metadata != metadata:
                print("metadata mismatch", file = report_file)
                print("metadata on download", s3_obj.metadata, file = report_file)
                print("expected metadata", metadata, file = report_file)
            if original_size != download_size:
                print("file size mismatch, original size: ", original_size, "download size: ", download_size , file = report_file)
            if original_size != s3_obj.content_length:
                print("file size mismatch, original size: ", original_size, "content_length: ", s3_obj.content_length , file = report_file)
            #report last_modifed anyway
            fetched_uuid = s3_obj.metadata['uuid']
            report_file.write(f'uuid: {fetched_uuid}, last modified {s3_obj.last_modified}, content_length: {s3_obj.content_length} \n')
            if s3_obj.last_modified < prev_time:
                print("last_modified is wrong. got, ", s3_obj.last_modified, "which cannot be less than: ", prev_time, file = report_file)
            e_tag_upload = common.get_MD5(upload_file)
            e_tag_download = common.get_MD5(download_file)
            if e_tag_download != e_tag_upload:
                print("file content md5 mismatch. uploaded file, ", e_tag_upload, " downloaded file, ", e_tag_download, file = report_file)
            if e_tag_upload != s3_obj.e_tag:
                print("e_tag is wrong as meta data, ", s3_obj.e_tag, "original file, ", e_tag_upload, file = report_file)
        end_time = datetime.datetime.now(tz = datetime.timezone.utc)
        deltatime = end_time - current_time
        report_file.write(f'end time: {end_time}, total used: {deltatime} \n')
        report_file.write("=====end of report==")

def test_with(credential_tag, endpoint_url = None, verify_cert = True):
    if not common.read_aws_credential(credential_tag):
        raise Exception("cannot find credential")
    
    aws_access_key = os.environ[common.key_tag]
    aws_secret_access = os.environ[common.secret_key_tag]
    s3_resource = common.get_s3_resource(aws_access_key, aws_secret_access, endpoint = endpoint_url, verify_ssl_cert = verify_cert)
    repeat = 400
    test(s3_resource, repeat= repeat)

def test_with_tagging(credential_tag, endpoint_url = None, verify_cert = True):
    if not common.read_aws_credential(credential_tag):
        raise Exception("cannot find credential")
    
    aws_access_key = os.environ[common.key_tag]
    aws_secret_access = os.environ[common.secret_key_tag]
    s3_resource = common.get_s3_resource(aws_access_key, aws_secret_access, endpoint = endpoint_url, verify_ssl_cert = verify_cert)
    s3_client = common.get_s3_client(aws_access_key, aws_secret_access, endpoint = endpoint_url, verify_ssl_cert = verify_cert)

    repeat = 400
    test_object_tagging(s3_resource, s3_client, repeat= repeat)

def test_with_copy_object(credential_tag, endpoint_url = None, verify_cert = True):
    if not common.read_aws_credential(credential_tag):
        raise Exception("cannot find credential")
    
    aws_access_key = os.environ[common.key_tag]
    aws_secret_access = os.environ[common.secret_key_tag]
    s3_resource = common.get_s3_resource(aws_access_key, aws_secret_access, endpoint = endpoint_url, verify_ssl_cert = verify_cert)
    s3_client = common.get_s3_client(aws_access_key, aws_secret_access, endpoint = endpoint_url, verify_ssl_cert = verify_cert)

    repeat = 400
    copy_object(s3_resource, s3_client, repeat= repeat)