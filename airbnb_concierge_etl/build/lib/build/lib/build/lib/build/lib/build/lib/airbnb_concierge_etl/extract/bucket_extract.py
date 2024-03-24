import pandas as pd
import boto3
from datetime import datetime
from typing import Optional


class BucketPull(object):
    """Retrieve data from S3 bucket."""

    def __init__(self, endpoint_url: Optional[str], username: Optional[str], pwd: Optional[str], bucket_name: str,
                 start_datetime: datetime):
        """
        The BucketPull class initialization.

        Args:
            endpoint_url: Bucket end point.
            username: access username.
            pwd: acess pwd.
            bucket_name: Bucket name
            start_datetime: Start datetime
        """
        self.endpoint_url = endpoint_url
        self.username = username
        self.pwd = pwd
        self.bucket_name = bucket_name
        self.start_datetime = start_datetime

    def bucket_s3_download_files(self) -> pd.DataFrame:
        """Request S3 bucket to get all files."""
        s3 = boto3.resource('s3',
                            endpoint_url=self.endpoint_url,
                            aws_access_key_id=self.username,
                            aws_secret_access_key=self.pwd)
        bucket = s3.Bucket(self.bucket_name)
        df_list = [pd.read_csv(obj['Body']) for obj in bucket.objects.all() if
                   obj.key.endswith('.csv') & (obj.last_modified >= self.start_datetime)]
        data = pd.concat(df_list)
        return data
