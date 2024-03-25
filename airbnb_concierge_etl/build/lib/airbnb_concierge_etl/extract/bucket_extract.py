import pandas as pd
import boto3
from datetime import datetime
import pytz
from typing import Optional


class BucketPull(object):
    """Retrieve data from S3 bucket."""

    def __init__(self, s3_service_resource: boto3.resource, bucket_name: str, start_datetime: datetime):
        """
        The BucketPull class initialization.

        Args:
            bucket_name: Bucket name
            start_datetime: Start datetime
        """
        self.s3_service_resource = s3_service_resource
        self.bucket_name = bucket_name
        self.start_datetime = start_datetime

    def bucket_s3_download_files(self) -> pd.DataFrame:
        """Request S3 bucket to get all files."""
        bucket = self.s3_service_resource.Bucket(self.bucket_name)
        df_list = [pd.read_csv(obj.get()['Body']) for obj in bucket.objects.all() if
                   obj.key.endswith('.csv') & (obj.last_modified >= self.start_datetime.replace(tzinfo=pytz.UTC))]

        data = pd.concat(df_list)
        return data
