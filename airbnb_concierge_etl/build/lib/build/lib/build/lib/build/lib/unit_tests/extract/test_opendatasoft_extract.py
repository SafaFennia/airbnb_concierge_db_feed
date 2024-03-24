from airbnb_concierge_etl.extract.opendatasoft_extract import OpendatasoftPull
import unittest.mock as mock
import pytest
from requests import HTTPError

def test_opendatasoft_pull_should_call_api_with_right_params():
    # Given
    session = mock.Mock()
    session.get.return_value = mock.Mock(**{"text": "1;2"})
    base_url = "https://public.opendatasoft.com/api/records/1.0/download/"
    filter_key = "column_19"
    filter_values = ["France"]
    dataset = 'air-bnb-listings'
    opendatasoft_pull = OpendatasoftPull(base_url, dataset, filter_key, filter_values, session)
    # When
    _ = opendatasoft_pull.opendatasoft_download_api_call()

    # Then
    session.get.assert_called_with(url=mock.ANY, params=[('dataset', 'air-bnb-listings'), ('q', 'column_19==France')])


def test_opendatasoft_pull_raises_an_exception_when_status_code_not_ok():
    # Given
    session = mock.Mock()
    response = mock.Mock()
    response.raise_for_status.side_effect = HTTPError()
    session.get.return_value = response
    base_url = "https://public.opendatasoft.com/api/records/1.0/download/"
    filter_key = "column_19"
    filter_values = ["France"]
    dataset = 'air-bnb-listings-not-found'
    opendatasoft_pull = OpendatasoftPull(base_url, dataset, filter_key, filter_values, session)

    # Then
    with pytest.raises(HTTPError):
        _ = opendatasoft_pull.opendatasoft_download_api_call()