from dotenv import load_dotenv
import os
from sqlalchemy.orm.session import sessionmaker
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine
from contextlib import contextmanager
from airbnb_concierge_etl.transform.client import raw_client_to_client_object
from logging import getLogger
from functools import wraps

_logger = getLogger(__name__)


def get_engine(echo=False, connect_args=None) -> Engine:
    """Return SQLAlchemy core engine.

    Connection URL is set to BUSINESS_OBJECTS_DB_SQLALCHEMY_ENGINE environment variable.
    """
    if connect_args is None:
        connect_args = {}
    return create_engine(
        os.environ.get('SANDBOX_DB_SQLALCHEMY_ENGINE', "postgresql://postgres:dev@localhost:5432/sandbox"), echo=echo,
        connect_args=connect_args,
    )


def get_session_factory(echo=False):
    """Return SQLAlchemy core `sessionmaker`, bind to engine from get_engine."""
    engine = get_engine(echo)
    return sessionmaker(bind=engine)


@contextmanager
def session_scope(echo=False):
    """Return SQLAlchemy session that can be used in context manager. Useful to request database in tests.

    See documentation: https://docs.sqlalchemy.org/en/13/orm/session_basics.html#getting-a-session
    """
    session_factory = get_session_factory(echo=echo)
    session = session_factory()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def log_start_extract_datetime(func):
    """Wrap function to add logging with input parameters `start_datetime` and `end_datetime`.

    Log level is INFO. Should be used as decorator @log_start_end_datetime on targeted functions.

    :param func: function to wrap with parameters logging
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        _logger.info(f"Data extraction start_datetime value is: {kwargs.get('start_datetime')}")
        func(*args, **kwargs)

    return wrapper


_ = load_dotenv()
_endpoint_url = os.environ['ENDPOINT_URL']
_username = os.environ['USERNAME']
_pwd = os.environ['PWD']
_bucket_name = os.environ['BUCKET_NAME']
_sandbox_engine = os.environ['_SANDBOX_DB_SQLALCHEMY_ENGINE']


def clients_upsert_pipe(start_date):
    """Init and run Clients Pipe.

    """
    clients = raw_client_to_client_object(_endpoint_url, _username, _pwd, _bucket_name, start_date)
    session = get_session_factory()
    for c in clients:
        try:
            session.merge(c)
            session.commit()
            _logger.info(f"Client {c.id_} inserted.")
        except Exception:
            _logger.exception(f"Client {c.id_} insert failed.")
    session.close()
    _logger.info("Client upsert pipe is done.")