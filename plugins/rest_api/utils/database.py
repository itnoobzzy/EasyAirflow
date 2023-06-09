import contextlib
from typing import Callable
from functools import wraps

from sqlalchemy import create_engine as _sa_create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

from rest_api import config

Base = declarative_base()


__all__ = [
    "DatabaseManager",
]


class DatabaseManager:
    """
    连接元数据库的类，在__init__中进行初始化
    """
    def __init__(self):
        self._engine = None
        self.session = None
        self.initialize()

    @property
    def engine(self) -> Engine:
        return self._engine

    @classmethod
    def create_database_engine(cls) -> Engine:
        return _sa_create_engine(config.LANDSAT_SQL_ALCHEMY_CONN,
                                 pool_pre_ping=True,
                                 pool_recycle=3600
                                 )

    def initialize(self, db_engine: Engine=None, scope_function: Callable = None):
        """
        Configure class for creating scoped sessions.
        :param db_engine: DB connection engine.
        :param scope_function: a function for scoping database connections.
        """
        # Set or initialize the database engine
        if db_engine is None:
            self._engine = self.create_database_engine()
        else:
            self._engine = db_engine

        # Create the session factory classes
        self.session = scoped_session(
            sessionmaker(bind=self._engine, expire_on_commit=False),
            scopefunc=scope_function)

        self.OnCommitExpiringSession = scoped_session(
            sessionmaker(bind=self._engine, expire_on_commit=True),
            scopefunc=scope_function)

    def cleanup(self):
        """
        Cleans up the database connection pool.
        """
        if self._engine is not None:
            self._engine.dispose()


db = DatabaseManager()


@contextlib.contextmanager
def create_session():
    """
    Contextmanager that will create and teardown a session.
    """
    session = db.session
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def landsat_provide_session(func):
    """
    Function decorator that provides a session if it isn't provided.
    If you want to reuse a session or run the function as part of a
    database transaction, you pass it to the function, if not this wrapper
    will create one and close it for you.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        arg_session = 'session'

        func_params = func.__code__.co_varnames
        session_in_args = arg_session in func_params and \
            func_params.index(arg_session) < len(args)
        session_in_kwargs = arg_session in kwargs

        if session_in_kwargs or session_in_args:
            return func(*args, **kwargs)
        else:
            with create_session() as session:
                kwargs[arg_session] = session
                return func(*args, **kwargs)

    return wrapper
