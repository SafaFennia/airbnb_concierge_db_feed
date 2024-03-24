import pandas as pd
import requests
import io
from typing import Iterable, Union


class OpendatasoftPull(object):
    """Retrieve data from opendatasoft API."""

    def __init__(self, base_url: str, dataset: str, filter_key: str, filter_values: Iterable[str],
                 session: Union[None, requests.Session] = None):
        """
        The OpendatasoftPull class initialization.

        Args:
            base_url: Opendatasoft API base url.
            dataset: opendatasoft dataset to call.
            session: requests.session to use ; default will create a new one.
        """
        self.base_url = base_url
        self.dataset = dataset
        self.filter_key = filter_key
        self.filter_values = filter_values
        self.session = session

    def opendatasoft_download_api_call(self) -> pd.DataFrame:
        """Request API to get all ecov_connected networks (filter on field 'role')."""
        session = self.session or requests.Session()
        query_param = " OR ".join([f"{self.filter_key}=={v}" for v in self.filter_values])
        parameters = [("dataset", self.dataset), ("q", query_param)]
        response = session.get(url=self.base_url, params=parameters)
        response.raise_for_status()
        return pd.read_csv(io.StringIO(response.text), sep=";")
