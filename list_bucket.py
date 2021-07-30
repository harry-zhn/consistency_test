#!python3
import os, sys
import argparse

from botocore import args, endpoint
import common


def test(aws_access_key, aws_secret_access, verify_cert = True, endpoint_url = None):
    s3_client = common.get_s3_client(aws_access_key, aws_secret_access, endpoint = endpoint_url,  verify_ssl_cert = verify_cert)
    response = common.list_objects(s3_client, common.bucket_name)
    print(response)
    
    s3_resource = common.get_s3_resource(aws_access_key, aws_secret_access, endpoint = endpoint_url, verify_ssl_cert = verify_cert)
    bucket = common.get_bucket(s3_resource, common.bucket_name)
    common.upload_file(s3_resource, common.bucket_name, "temp1", '/Users/harryzhang/README.md', lambda: print())
    for obj in bucket.objects.all():
        print(obj.key, obj.size, obj.owner, obj.meta, obj.last_modified)

def test_with(credential_tag, endpoint_url = None, verify_cert = True):
    print("=================================", credential_tag, "==============================")
    if not common.read_aws_credential(credential_tag):
        raise Exception("cannot find credential")
    
    aws_access_key = os.environ[common.key_tag]
    aws_secret_access = os.environ[common.secret_key_tag]
    test(aws_access_key, aws_secret_access, verify_cert = verify_cert, endpoint_url = endpoint_url)

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
