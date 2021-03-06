from threading import Thread

import argparse
import os, sys
import datetime
import time
import common

import botocore


def read_obj_tagging(s3_client, object_key = common.object_key_for_overwrite, repeat = 200):
    sleeping_time = 0.25 # for 50 millisec
    current_time = datetime.datetime.now(tz = datetime.timezone.utc)
    current_date = current_time.date()
    time_stamp = current_time.time()

    local_work_dir = os.path.dirname(os.path.abspath(__file__))
    download_dir = os.path.join(local_work_dir, "test_data/download")
    common.prepare_local_folder(download_folder=download_dir)
    object_key = common.object_key_for_tagging

    part_of_filename = f'{current_date.year:04}-{current_date.month:02}-{current_date.day:02}_{time_stamp.hour:02}-{time_stamp.minute:02}-{time_stamp.second:02}'
    report_filename = os.path.join(local_work_dir, "test_data", f'{part_of_filename}_read_tagging_report.txt')

    with open(report_filename, "x") as report_file:
        report_file.write(f"read tagging test on key {object_key} at {current_time} \n")
        report_file.write(f'trying to repeat {repeat} times\n')
        prev_tag = {}
        for step in range(repeat):
            response = s3_client.get_object_tagging(Bucket = common.bucket_name, Key = object_key)
            tag_list_returned = response['TagSet']
            if prev_tag: 
                #report error
                if len(prev_tag) != len(tag_list_returned):
                    print(f"at step {step}, expect to have {len(prev_tag)}, Got {len(tag_list_returned)}", file=report_file)
                else:
                    for tag in tag_list_returned:
                        if prev_tag[tag['Key']] > int(tag['Value']):
                            print(f"At step {step}, Expect to have value no less than {prev_tag[tag['Key']]}, got {tag['Value']}", file=report_file)
            # convert returned tags to a lookup table
            for tag in tag_list_returned:
                prev_tag[tag['Key']] = int(tag['Value'])

            time.sleep(sleeping_time)

        end_time = datetime.datetime.now(tz = datetime.timezone.utc)
        deltatime = end_time - current_time
        report_file.write(f'end time: {end_time}, total used: {deltatime} \n')
        report_file.write("=====end of report==")


def test(s3_resource, object_key = common.object_key_for_overwrite, repeat = 200):
    sleeping_time = 0.25 # for 50 millisec

    local_work_dir = os.path.dirname(os.path.abspath(__file__))
    download_dir = os.path.join(local_work_dir, "test_data/download")
    common.prepare_local_folder(download_folder=download_dir)
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
                e_tag = s3_obj.e_tag
                print(f'key: {s3_obj.key}, e_tag: {e_tag}, get e_tag at: {datetime.datetime.now(datetime.timezone.utc)}', file=report_file)
                s3_obj.download_file(download_file)
                print(f'key: {s3_obj.key}, e_tag: {e_tag}, finished file download at: {datetime.datetime.now(datetime.timezone.utc)}', file = report_file)
                #report file info here
                fetched_uuid = s3_obj.metadata['uuid']
                e_tag_download = common.get_MD5(download_file)
                print(f'key: {s3_obj.key}, comparing e_tag at:{datetime.datetime.now(tz = datetime.timezone.utc)}', file=report_file)
                if e_tag_download != e_tag:
                    print("e_tag is wrong as meta data, ", e_tag, "original file, ", e_tag_download, file = report_file)
                report_file.write(f'uuid: {fetched_uuid}, last modified {s3_obj.last_modified}, content_length: {s3_obj.content_length}, etag:{s3_obj.e_tag} \n')
            except botocore.exceptions.ClientError as error:
                response = error.response
                print(f"Error happens ..., Error-Code: {response['Error']['Code']}, HTTP Status Code: {response['ResponseMetadata']['HTTPStatusCode']} ")

            time.sleep(sleeping_time)
        
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
    repeat = 200
    test(s3_resource, repeat= repeat)

def test_with_delete(credential_tag, endpoint_url = None, verify_cert = True):
    if not common.read_aws_credential(credential_tag):
        raise Exception("cannot find credential")
    
    aws_access_key = os.environ[common.key_tag]
    aws_secret_access = os.environ[common.secret_key_tag]
    s3_resource = common.get_s3_resource(aws_access_key, aws_secret_access, endpoint = endpoint_url, verify_ssl_cert = verify_cert)
    repeat = 200
    test(s3_resource, object_key= common.object_key_read_after_delete, repeat= repeat)

def test_with_tagging(credential_tag, endpoint_url = None, verify_cert = True):
    if not common.read_aws_credential(credential_tag):
        raise Exception("cannot find credential")
    
    aws_access_key = os.environ[common.key_tag]
    aws_secret_access = os.environ[common.secret_key_tag]
    s3_client = common.get_s3_client(aws_access_key, aws_secret_access, endpoint = endpoint_url, verify_ssl_cert = verify_cert)
    repeat = 300
    read_obj_tagging(s3_client, object_key= common.object_key_read_after_delete, repeat= repeat)
