from airbnb_concierge_etl.airbnb_concierge_etl.extract.opendatasoft_extract import OpendatasoftPull
import unittest.mock as mock

def test_opendatasoft_pull_should_call_api_with_right_params():
    # Given
    session = mock.Mock()
    session.get.return_value = mock.Mock(**{"text": "1;2"})
    base_url = "https://public.opendatasoft.com/api/records/1.0/download/"
    filter_key = "column_19"
    filter_values = ["France"]
    dataset = 'air-bnb-listings'
    opendatasoft_pull = OpendatasoftPull(base_url, dataset, filter_key, filter_values)
    # When
    _ = opendatasoft_pull.opendatasoft_download_api_call()

    # Then
    session.get.assert_called_with(params=[
        ("dataset", "air-bnb-listings"),
        ("q", "column_19==France"),
    ], url=mock.ANY)