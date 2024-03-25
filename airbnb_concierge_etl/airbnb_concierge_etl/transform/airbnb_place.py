"""
This module provides functions to map airbnb places (from Opendatasoft) to AirbnbPlace sandbox object.
"""

from logging import getLogger
from typing import List, Tuple

import pandas as pd
from airbnb_concierge_etl.model.sandbox import AirbnbPlace
from airbnb_concierge_etl.extract.opendatasoft_extract import OpendatasoftPull

_logger = getLogger(__name__)


def raw_airbnb_place_to_airbnb_place_object(dataset, filter_key, filter_values) -> Tuple[List[AirbnbPlace], pd.DataFrame]:

    """
    Map raw airbnb places to AirbnbPlace objects.

    Args:
        dataset: opendatasoft dataset name to call.
        filter_key: field on which the filter will be applied.
        filter_values: filter vales
    """

    opendatasoft_pull = OpendatasoftPull(dataset=dataset, filter_key=filter_key,
                                         filter_values=filter_values, session=None)
    raw_airbnb_places = opendatasoft_pull.opendatasoft_download_api_call()
    raw_airbnb_places.rename(columns={'column_19': 'country'}, inplace=True)

    return [
        AirbnbPlace(
            id_=int(row.id),
            host_id=row.host_id,
            room_type=row.room_type,
            room_price=row.room_price,
            updated_date=row.updated_date,
            city=row.city,
            country=row.country,
        ) for row in raw_airbnb_places.itertuples(index=False)
    ], raw_airbnb_places
