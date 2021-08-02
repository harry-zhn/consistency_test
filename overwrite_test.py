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

import argparse
import os, sys
import uuid
import random
import datetime
import common

def test(s3_resource, repeat = 200):
    local_work_dir = os.path.dirname(os.path.abspath(__file__))
    upload_dir = os.path.join(local_work_dir, "test_data/upload")
    download_dir = os.path.join(local_work_dir, "test_data/download")
    object_key = common.object_key_for_overrite
    int_range = 2 ** 10
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
            download_file = os.path.join(download_dir, short_filename)
            with open(upload_file, 'x') as source:
                source.write(f"overwrite test at {current_time}")
                loop_count = random.randrange(10, 30)
                for i in range(loop_count):
                    num = random.randint(-int_range, int_range)
                    source.write(str.format("{} \n",num))
            metadata  = {'prop1': str(random.randint(-int_range, int_range)),
                         'prop2': str(random.randint(-int_range, int_range)),
                         'uuid': str(uuid.uuid4())
                         }
            original_size = os.stat(upload_file).st_size
            extra_args = {"Metadata": metadata}
            s3_resource.Object(common.bucket_name, object_key).upload_file(upload_file, ExtraArgs = extra_args )
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
    print("================================= testing with credential: ", credential_tag, "==============================")
    if not common.read_aws_credential(credential_tag):
        raise Exception("cannot find credential")
    
    aws_access_key = os.environ[common.key_tag]
    aws_secret_access = os.environ[common.secret_key_tag]
    s3_resource = common.get_s3_resource(aws_access_key, aws_secret_access, endpoint = endpoint_url, verify_ssl_cert = verify_cert)
    repeat = 200
    test(s3_resource, repeat= repeat)
    print("=================DONE=============")


flag_aws_credential = "aws_credential"
flag_endpoint_url = "endpoint_url"
flag_no_ssl_certificate = "no_ssl_certificate"
parser = argparse.ArgumentParser()
parser.add_argument(str.format("--{0}", flag_aws_credential), default = 'default', help ="specify a credential from ~/.aws/credentials")
parser.add_argument(str.format("--{0}", flag_endpoint_url), default=None, help = "optional endpoint url")

# set default as True, and when specified, set the value to True
parser.add_argument(str.format("--{0}", flag_no_ssl_certificate), action = 'store_false', help="If the certificate on the server should be verified with api call")
args = parser.parse_args()
if not hasattr(args, flag_aws_credential):
    parser.print_help()
    sys.exit(0)

aws_credential = getattr(args, flag_aws_credential)

#endpoint_url = None
endpoint_url = getattr(args, flag_endpoint_url)

#verify_certificate = True
verify_certificate = getattr(args, flag_no_ssl_certificate)

test_with(aws_credential, endpoint_url = endpoint_url, verify_cert = verify_certificate)
