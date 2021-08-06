import argparse
import sys

import list_bucket
import read_test
import overwrite_test
import read_after_delete_test
import multipart_upload

if __name__ == "__main__":
    flag_aws_credential = "aws_credential"
    flag_endpoint_url = "endpoint_url"
    flag_no_ssl_certificate = "no_ssl_certificate"
    flag_test_name = "test_name"
    parser = argparse.ArgumentParser()
    parser.add_argument(f"--{flag_aws_credential}", default = 'default', help ="specify a credential d from ~/.aws/credentials")
    parser.add_argument(f"--{flag_endpoint_url}", default=None, help = "optional endpoint url")
    parser.add_argument(f"--{flag_test_name}", default=None, help = "name of the test to run")

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

    dic_of_tests = {
        "overwrite" : overwrite_test.test_with,
        "overwrite-tagging" : overwrite_test.test_with_tagging,
        "overwrite-with-copy": overwrite_test.test_with_copy_object,
        "remote-read" : read_test.test_with,
        "remote-read-tagging" : read_test.test_with_tagging,

        "remote-read-after-delete": read_test.test_with_delete,
        "read-after-delete": read_after_delete_test.test_with,

        "list-v2-with-write": list_bucket.test_with_write_and_list_v2,
        "remote-list-v2": list_bucket.test_with_list_v2,

        "list-with-write": list_bucket.test_with_write_and_list,
        "remote-list": list_bucket.test_with_list,

        "deletes-and-list": list_bucket.test_with_deletes_and_list,

        "multipart-upload-create": multipart_upload.test_with,
        "multipart-uploads": multipart_upload.test_with_uploads,
    }

    test_name = getattr(args, flag_test_name)
    if not test_name or test_name not in dic_of_tests:
        parser.print_usage()
        print("Available tests: ", list(dic_of_tests.keys()))
        sys.exit(0)

    # run the test
    print("test name:", test_name)
    print("test with credential:", aws_credential)
    dic_of_tests[test_name](aws_credential, endpoint_url = endpoint_url, verify_cert = verify_certificate)
    print("=================DONE=============")