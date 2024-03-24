import pytest
import os
import boto3
from moto import mock_aws
from datetime import datetime
from airbnb_concierge_etl.extract.bucket_extract import BucketPull


@pytest.fixture(scope='function')
def aws_credentials():
    # Mocked AWS Credentials for Moto
    os.environ['aws_access_key_id'] = 'fake-username'
    os.environ['aws_secret_access_key'] = 'fake-pwd'


@pytest.fixture(scope='function')
def s3_mock(aws_credentials):
    with mock_aws():
        yield boto3.client('s3')


@mock_aws
def test_ls(s3_mock):
    """Test the custom s3 ls function mocking S3 with moto"""
    # GIVEN
    bucket = "testbucket"
    key = "testkey"
    body = "testing"
    s3_mock.create_bucket(Bucket=bucket)
    s3_mock.put_object(Bucket=bucket, Key=key, Body=body)
    bucket_pull = BucketPull(s3_mock, bucket, datetime.now())

    # WHEN
    output = bucket_pull.bucket_s3_download_files()
    #THEN
    assert len(output) == 1
    assert output[0] == 'testkey'
