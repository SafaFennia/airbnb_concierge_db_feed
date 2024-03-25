import pytest
import os
import boto3
from moto import mock_aws
from datetime import datetime, timedelta
import pytz
from airbnb_concierge_etl.extract.bucket_extract import BucketPull


@pytest.fixture(scope='function')
def aws_credentials():
    # Mocked AWS Credentials for Moto
    os.environ['aws_access_key_id'] = 'fake-username'
    os.environ['aws_secret_access_key'] = 'fake-pwd'


@pytest.fixture(scope='function')
def s3_mock_client(aws_credentials):
    with mock_aws():
        yield boto3.client('s3')


@pytest.fixture(scope='function')
def s3_mock_resource(aws_credentials):
    with mock_aws():
        yield boto3.resource('s3')


@mock_aws
def test_bucket_pull_should_retrieve_expected_data_from_bucket(s3_mock_client, s3_mock_resource):
    """Test the custom s3 ls function mocking S3 with moto"""
    # GIVEN
    bucket = "testbucket"
    key = "testkey.csv"
    body = "testing"
    s3_mock_client.create_bucket(Bucket=bucket)
    s3_mock_client.put_object(Bucket=bucket, Key=key, Body=body)
    bucket_pull = BucketPull(s3_mock_resource, bucket, datetime.now() - timedelta(days=1))

    # WHEN
    output = bucket_pull.bucket_s3_download_files()

    # THEN
    assert body in list(output.columns)
