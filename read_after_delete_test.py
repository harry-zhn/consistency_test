'''
To test consistency level regarding read-after-delete
    PutObject
    GetObject
    HeadObject
    CopyObject
Important data to verify
    object key,
        [size, last_modified, metadata]
        acl
0.0 to create or update an object
0.1 to try HeadObject, and GetObject
1.0 to delete the object
1.1 to try to GetObject/HeadObject


local record            | remote record   | notes
=====================================================================================
0.0                     | 0.1             | expect record[0.0] == record[0.1] 
-------------------------------------------------------------------------------------
1.0                     | 1.1             | exception should happen that key doesn't exist
                        |                 |  for 200 times, 
                        |                 |     expect record[1.1] == record[1.1]
'''

import os
import uuid
import random
import datetime
import common

import botocore

def test(s3_resource, repeat = 200):
    local_work_dir = os.path.dirname(os.path.abspath(__file__))
    upload_dir = os.path.join(local_work_dir, "test_data/upload")
    download_dir = os.path.join(local_work_dir, "test_data/download")
    object_key = common.object_key_read_after_delete
    int_range = 2 ** 10
    current_time = datetime.datetime.now(tz = datetime.timezone.utc)
    current_date = current_time.date()
    time_stamp = current_time.time()

    prev_time = current_time
    part_of_filename = f'{current_date.year:04}-{current_date.month:02}-{current_date.day:02}_{time_stamp.hour:02}-{time_stamp.minute:02}-{time_stamp.second:02}'
    report_filename = os.path.join(local_work_dir, "test_data", f'{part_of_filename}_delete_read_local_report.txt')
    with open(report_filename, "x") as report_file:
        report_file.write(f"read after delete local test on key {object_key} at {current_time} \n")
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
            #this is to make sure the object exists in the bucket
            s3_obj_put = s3_resource.Object(common.bucket_name, object_key)
            s3_obj_put.upload_file(upload_file, ExtraArgs = extra_args )

            #call delete
            response = s3_obj_put.delete()
            print(f"response status of DELETE: HTTP Status code {response['ResponseMetadata']['HTTPStatusCode']}, date: {response['ResponseMetadata']['HTTPHeaders']['date']}", file=report_file)
            try:
                s3_obj = s3_resource.Object(common.bucket_name, object_key)
                s3_obj.download_file(download_file)
                print(f"error: object should be deleted: {s3_obj.metadata}, {s3_obj.last_modified}, etag: {s3_obj.e_tag}, downloaded file, {download_file}", file = report_file)
            except botocore.exceptions.ClientError as error:
                response = error.response
                print(f"file is deleted as expected..., Error-Code: {response['Error']['Code']}, HTTP Status Code: {response['ResponseMetadata']['HTTPStatusCode']} ")
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