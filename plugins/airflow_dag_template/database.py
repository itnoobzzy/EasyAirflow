#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:wanglong

from typing import Callable

from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import sessionmaker, scoped_session


__all__ = [
    "DBManger",
]


class DBManger(object):
    """
    连接元数据库的类，在__init__中进行初始化
    """
    def __init__(self,url):
        self.url = url
        self._engine = None
        self.session_factory = None
        self.initialize()

    @property
    def engine(self) -> Engine:
        return self._engine

    def initialize(self, db_engine: Engine=None, scope_function: Callable = None):
        """
        Configure class for creating scoped sessions.
        :param db_engine: DB connection engine.
        :param scope_function: a function for scoping database connections.
        """
        # Set or initialize the database engine
        if db_engine is None:
            engine_args = {}
            engine_args['pool_size'] = 5
            engine_args['pool_recycle'] = 1800
            engine_args['pool_pre_ping'] = True
            engine_args['max_overflow'] = 10

            self._engine = create_engine(self.url, **engine_args)
        else:
            self._engine = db_engine

        # Create the session factory classes
        self.session_factory = scoped_session(
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
