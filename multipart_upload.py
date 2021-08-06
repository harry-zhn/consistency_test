'''
Tests
1.12    CreateMultiPartUpload           | yes *     | local test
1.12.1  UploadPart                      | no       | local test 
1.12.2  UploadPartCopy                  | yes       | local test
1.12.3  ListParts                       | yes       | local test
1.12.4  ListMultipartUploads            | yes       | local test
1.12.5  CompleteMultipartUpload         | yes       | local test
1.12.6  AbortMultipartUpload            | yes       | local test
'''

import os
import datetime

import common

import botocore

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

def upload_parts(s3_client):
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
        report_file.write(f"MultipartUpload upload, complete, and abort test on at {current_time} \n")
        report_file.write(f'trying to create  MultipartUploadFirst\n')

        object_key = f'{common.object_key_prefix_for_multipart_upload}_upload_test'
        acl =  'private' #'private|authenticated-read'
        response = s3_client.create_multipart_upload(ACL=acl, Bucket = common.bucket_name, Key = object_key)
        upload_id = response['UploadId']
        steps = 10
        multipart_uploads = {'Parts':[]}
        for step in range(steps):
            response, uploaded_e_tag, file_size = common.upload_multipart_part_with_random_data(s3_client, common.bucket_name, object_key, int(step + 1), upload_id )
            #each part must be 5 MB at least.
            print(f'ETag returned: {response["ETag"]},  uploaded_e_tag: {uploaded_e_tag}, file size: {file_size/1024/1024}', file=report_file)
            multipart_uploads['Parts'].append({'ETag':response['ETag'], 'PartNumber': step + 1})

        try:
            response = s3_client.complete_multipart_upload(Bucket = common.bucket_name, Key = object_key, MultipartUpload = multipart_uploads, UploadId = upload_id)
            print(f"completed multipart upload. {response['Location']}, Etag:{response['ETag']}, Bucket: {response['Bucket']} Key: {response['Key']}")
        except botocore.exceptions.ClientError as error:
            response = error.response
            print(f"Error Code: {response['Error']['Code']}")
            print(response)
        response = s3_client.list_multipart_uploads(Bucket = common.bucket_name)
        if 'Uploads' in response:
            print(f"Error: there should be no MultipartUploads anymore. {response['Uploads']}",file= report_file)
        
        # test for abort before complete
        response = s3_client.create_multipart_upload(ACL=acl, Bucket = common.bucket_name, Key = object_key)
        upload_id = response['UploadId']
        steps = 5
        multipart_uploads = {'Parts':[]}
        for step in range(steps):
            response, uploaded_e_tag, file_size = common.upload_multipart_part_with_random_data(s3_client, common.bucket_name, object_key, int(step + 1), upload_id )
            #each part must be 5 MB at least.
            print(f'ETag returned: {response["ETag"]},  uploaded_e_tag: {uploaded_e_tag}, file size: {file_size/1024/1024}', file=report_file)
            multipart_uploads['Parts'].append({'ETag':response['ETag'], 'PartNumber': step + 1})
        response = s3_client.list_parts(Bucket = common.bucket_name, Key = object_key, UploadId = upload_id)
        if 'Parts' not in response or len(response['Parts']) != steps:
            print(f"error: expect to have {steps} parts, Got {0 if 'Parts' not in response else len(response['Parts'])}")
        if 'Parts' in response:
            for part in response['Parts']:
                print(f'{part}', file=report_file)
        s3_client.abort_multipart_upload(Bucket = common.bucket_name, Key = object_key, UploadId = upload_id )
        response = s3_client.list_multipart_uploads(Bucket = common.bucket_name)
        if 'Uploads' in response:
            print(f"error: expect no MultipartUploads. Got {len(response['Uploads'])}")
        try:
            response = s3_client.list_parts(Bucket = common.bucket_name, Key = object_key, UploadId = upload_id)
        except botocore.exceptions.ClientError as error:
            response = error.response
            error_code = response['ResponseMetadata']['HTTPStatusCode'] 
            if error_code != 404:
                print(f"error, expect to get 404 error code. got {error_code}")


        # we are expecting no any uploads returned
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

def test_with_uploads(credential_tag, endpoint_url = None, verify_cert = True):
    if not common.read_aws_credential(credential_tag):
        raise Exception("cannot find credential")
    
    aws_access_key = os.environ[common.key_tag]
    aws_secret_access = os.environ[common.secret_key_tag]
    s3_client = common.get_s3_client(aws_access_key, aws_secret_access, endpoint = endpoint_url, verify_ssl_cert = verify_cert)
    common.abort_all_multipart_uploads(s3_client, common.bucket_name)
    upload_parts(s3_client)
