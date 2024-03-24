import pytest
import os
import boto3
from moto import mock_aws
from src.s3funcs import s3_ls


@pytest.fixture(scope='function')
def aws_credentials():
    # Mocked AWS Credentials for Moto
    os.environ['BUCKET_URL'] = 'fakeurl'
    os.environ['BUCKET_USERNAME'] = 'fake-username'
    os.environ['BUCKET_PWD'] = 'fake-pwd'


@pytest.fixture(scope='function')
def s3_mock(aws_credentials):
    with mock_aws():
        yield boto3.client('s3')


@mock_aws
def test_ls(s3_boto):
    """Test the custom s3 ls function mocking S3 with moto"""

    bucket = "testbucket"
    key = "testkey"
    body = "testing"
    s3_boto.create_bucket(Bucket=bucket)
    s3_boto.put_object(Bucket=bucket, Key=key, Body=body)
    ls_output = s3_ls(Bucket=bucket, Prefix=key)
    assert len(ls_output) == 1
    assert ls_output[0] == 'testkey'