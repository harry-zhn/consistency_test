from threading import Thread

import argparse
import os, sys
import datetime
import time
import common

import botocore

def test(s3_resource, object_key = common.object_key_for_overwrite, repeat = 200):
    sleeping_time = 0.25 # for 50 millisec

    local_work_dir = os.path.dirname(os.path.abspath(__file__))
    download_dir = os.path.join(local_work_dir, "test_data/download")
    object_key = common.object_key_for_overrite
    current_time = datetime.datetime.now(tz = datetime.timezone.utc)
    current_date = current_time.date()
    time_stamp = current_time.time()

    part_of_filename = f'{current_date.year:04}-{current_date.month:02}-{current_date.day:02}_{time_stamp.hour:02}-{time_stamp.minute:02}-{time_stamp.second:02}'
    report_filename = os.path.join(local_work_dir, "test_data", f'{part_of_filename}_read_report.txt')
    with open(report_filename, "x") as report_file:
        report_file.write(f"read test on key {object_key} at {current_time} \n")
        report_file.write(f'trying to repeat {repeat} times\n')
        for step in range(repeat):
            short_filename = str.format('{0}_{1}', step, part_of_filename)
            download_file = os.path.join(download_dir, short_filename)
            s3_obj = s3_resource.Object(common.bucket_name, object_key)
            try:
                s3_obj.download_file(download_file)
                #report file info here
                fetched_uuid = s3_obj.metadata['uuid']
                e_tag_download = common.get_MD5(download_file)
                if e_tag_download != s3_obj.e_tag:
                    print("e_tag is wrong as meta data, ", s3_obj.e_tag, "original file, ", e_tag_download, file = report_file)
                report_file.write(f'uuid: {fetched_uuid}, last modified {s3_obj.last_modified}, content_length: {s3_obj.content_length}, etag:{s3_obj.e_tag} \n')
            except botocore.exceptions.clientError as error:
                response = error.response
                print(f"Error happens ..., Error-Code: {response['Error']['Code']}, HTTP Status Code: {response['ResponseMetadata']['HTTPStatusCode']} ")

            time.sleep(sleeping_time)
        
        end_time = datetime.datetime.now(tz = datetime.timezone.utc)
        deltatime = end_time - current_time
        report_file.write(f'end time: {end_time}, total used: {deltatime} \n')
        report_file.write("=====end of report==")

def test_with(credential_tag, endpoint_url = None, verify_cert = True):
    print("================================= testing with credential: ", credential_tag, "==============================")
    if not common.read_aws_credential(credential_tag):
        raise Exception("cannot find credential")
    
    aws_access_key = os.environ[common.key_tag]
    aws_secret_access = os.environ[common.secret_key_tag]
    s3_resource = common.get_s3_resource(aws_access_key, aws_secret_access, endpoint = endpoint_url, verify_ssl_cert = verify_cert)
    repeat = 200
    test(s3_resource, repeat= repeat)
    print("=================DONE=============")

def test_with_delete(credential_tag, endpoint_url = None, verify_cert = True):
    print("================================= testing with credential: ", credential_tag, "==============================")
    if not common.read_aws_credential(credential_tag):
        raise Exception("cannot find credential")
    
    aws_access_key = os.environ[common.key_tag]
    aws_secret_access = os.environ[common.secret_key_tag]
    s3_resource = common.get_s3_resource(aws_access_key, aws_secret_access, endpoint = endpoint_url, verify_ssl_cert = verify_cert)
    repeat = 200
    test(s3_resource, object_key= common.object_key_read_after_delete, repeat= repeat)
    print("=================DONE=============")
