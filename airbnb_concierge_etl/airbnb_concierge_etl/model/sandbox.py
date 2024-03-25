"""This module contains the sandbox definition.

This module defines the sandbox database schemas, thanks to SQLAlchemy ORM.

Attributes:
    Base: sqlalchemy declarative base class (see reference at:
            https://docs.sqlalchemy.org/en/14/orm/mapping_styles.html#declarative-mapping)
"""
import sqlalchemy as sa
from sqlalchemy.orm import relationship
from sqlalchemy.orm import declarative_base
import enum

Base = declarative_base()


class RoomType(enum.Enum):
    """Defines Airbnb romm types."""

    entire_home_apt = "Entire home/apt"
    shared_room = "Shared room"
    private_room = "Private room"
    hotel_room = 'Hotel room'


class Client(Base):
    """Contains clients information."""

    __tablename__ = 'clients'

    # Unique RegisteredUser id, equivalent to backend users.id
    id_ = sa.Column(sa.Integer, primary_key=True, name='id')
    address = sa.Column(sa.String)
    city = sa.Column(sa.String)
    zip = sa.Column(sa.String)
    created_at = sa.Column(sa.Date)


class AirbnbPlace(Base):
    """Contains Airbnb places information."""

    __tablename__ = 'airbnb_places'

    # Unique RegisteredUser id, equivalent to backend users.id
    id_ = sa.Column(sa.Integer, primary_key=True, name='id')
    host_id = sa.Column(sa.Integer)
    room_type = sa.Column(sa.Enum(RoomType))
    room_price = sa.Column(sa.Integer)
    updated_date = sa.Column(sa.Date)
    city = sa.Column(sa.String)
    country = sa.Column(sa.String)
