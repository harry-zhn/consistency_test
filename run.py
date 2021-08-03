import argparse
import sys

import read_test
import overwrite_test
import read_after_delete_test

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
        "remote-read" : read_test.test_with,
        "remote-read-after-delete": read_test.test_with_delete,
        "read-after-delete": read_after_delete_test.test_with,
    }

    test_name = getattr(args, flag_test_name)
    if not test_name or test_name not in dic_of_tests:
        parser.print_usage()
        print("Available tests: ", list(dic_of_tests.keys))
        sys.exit(0)

    # run the test
    print("test name:", test_name)
    dic_of_tests[test_name](aws_credential, endpoint_url = endpoint_url, verify_cert = verify_certificate)