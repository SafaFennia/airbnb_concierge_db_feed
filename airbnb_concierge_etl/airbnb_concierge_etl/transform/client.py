"""
This module provides functions to map client information (from bucket) to Client sandbox object.
"""
import datetime
from logging import getLogger
from typing import List, Union
import requests
import boto3
import pandas as pd
from airbnb_concierge_etl.model.sandbox import Client
from airbnb_concierge_etl.extract.bucket_extract import BucketPull

_logger = getLogger(__name__)


def _get_city_name_from_address_and_zip_code(address: str, zip_code: str) -> Union[None, str]:
    """Get city name from zip code using data.gouv API"""

    session = requests.Session()
    base_url = "https://api-adresse.data.gouv.fr/search/"
    params = {k: v for k, v in (('q', address), ('limit', 1), ('postcode', zip_code))}

    resp = session.get(base_url, params=params)
    if resp.status_code != 200:
        _logger.warning(f"api call failed, got Error {resp.status_code} from api for request {resp.url}")
        return None
    elif len(resp.json()['features']) == 0:
        _logger.warning(f"No result for request {resp.url}")
        return None
    else:
        resp_json = resp.json()
        return resp_json['features'][0]['properties']['city']


def raw_client_to_client_object(endpoint_url: str, username: str, pwd: str, bucket_name: str,
                                start_datetime: datetime.datetime) -> List[Client]:
    """Map raw bucket clients information to Client objects.

    Args:
     endpoint_url: bucket end point url
     username: bucket username
     pwd: bucket password
     bucket_name: bucket name
     start_datetime: start datetime to be used to filter bucket data

    """
    s3 = boto3.resource('s3',
                        endpoint_url=endpoint_url,
                        aws_access_key_id=username,
                        aws_secret_access_key=pwd)
    bucket_pull = BucketPull(s3, bucket_name, start_datetime)
    raw_clients = bucket_pull.bucket_s3_download_files()
    raw_clients['city'] = raw_clients.apply(
        lambda x: _get_city_name_from_address_and_zip_code(x.address, x.zip) if pd.isna(x.city) else x.city, axis=1)

    return [
        Client(
            id_=int(row.id),
            address=row.address,
            city=row.city,
            zip=row.zip,
            created_at=row.created_at
        ) for row in raw_clients.itertuples(index=False)
    ]
