'''
Tests
1.12    CreateMultiPartUpload           | yes *     | local test
1.12.1  UploadPart                      | yes       | local test 
1.12.2  UploadPartCopy                  | yes       | local test
1.12.3  ListParts                       | yes       | local test
1.12.4  ListMultipartUploads            | yes       | local test
1.12.5  CompleteMultipartUpload         | yes       | local test
1.12.6  AbortMultipartUpload            | yes       | local test
'''

import os
import datetime


import common

def test_multipart_create_and_delete(s3_client, repeat = 200):

    local_work_dir = os.path.dirname(os.path.abspath(__file__))
    upload_dir = os.path.join(local_work_dir, "test_data/upload")
    download_dir = os.path.join(local_work_dir, "test_data/download")
    common.prepare_local_folder(upload_folder=upload_dir, download_folder=download_dir)
    
    current_time = datetime.datetime.now(tz = datetime.timezone.utc)
    current_date = current_time.date()
    time_stamp = current_time.time()
    part_of_filename = f'{current_date.year:04}-{current_date.month:02}-{current_date.day:02}_{time_stamp.hour:02}-{time_stamp.minute:02}-{time_stamp.second:02}'

    report_filename = os.path.join(local_work_dir, "test_data", f'{part_of_filename}_multipart_create-report.txt')

    with open(report_filename, "x") as report_file:
        report_file.write(f"MultipartUpload create and delete test on at {current_time} \n")
        report_file.write(f'trying to create {repeat} MultipartUploadFirst\n')
        objects = set()
        acl = 'private' # 'private|authenticated-read'
        for step in range(repeat):
            object_key = f'{common.object_key_prefix_for_multipart_upload}{step:03}'
            response = s3_client.create_multipart_upload(ACL=acl, Bucket = common.bucket_name, Key = object_key)
            objects.add( (response['Key'], response['UploadId']) )
        print(objects)
        #delete test
        while objects:
            print(objects)
            object_key, upload_id = objects.pop()
            response = s3_client.abort_multipart_upload(Bucket = common.bucket_name, Key = object_key, UploadId = upload_id )
            print("deleting", upload_id, object_key)#, file=report_file)

            response = s3_client.list_multipart_uploads(Bucket = common.bucket_name)
            if 'Uploads' not in response:
                if len(objects) > 0: 
                    report_file.write(f'expect to have {len(objects)} multipart uploads. Got {len(uploads)}\n')
                break

            uploads = response['Uploads']
            if len(uploads) != len(objects):
                report_file.write(f'expect to have {len(objects)} multipart uploads. Got {len(uploads)}\n')
                for upload in uploads:
                    print("upload_id", upload['UploadId'], "key", upload['Key'], file=report_file)
        
        end_time = datetime.datetime.now(tz = datetime.timezone.utc)
        deltatime = end_time - current_time
        report_file.write(f'end time: {end_time}, total used: {deltatime} \n')
        report_file.write("=====end of report==")

def test_with(credential_tag, endpoint_url = None, verify_cert = True):
    if not common.read_aws_credential(credential_tag):
        raise Exception("cannot find credential")
    
    aws_access_key = os.environ[common.key_tag]
    aws_secret_access = os.environ[common.secret_key_tag]
    s3_client = common.get_s3_client(aws_access_key, aws_secret_access, endpoint = endpoint_url, verify_ssl_cert = verify_cert)
    common.abort_all_multipart_uploads(s3_client, common.bucket_name)
    repeat = 5
    test_multipart_create_and_delete(s3_client, repeat= repeat)
