#!/usr/bin/env python
# -*- coding:utf-8 -*-

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from functools import wraps
import os
import contextlib

from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
import pendulum

from airflow_dag_template.database import DBManger


# The base class which our objects will be defined on.
Base = declarative_base()

LANDSAT_SQL_ALCHEMY_CONN = os.environ.get("LANDSAT_SQL_ALCHEMY_CONN")

landsat_db = DBManger(LANDSAT_SQL_ALCHEMY_CONN)


@contextlib.contextmanager
def create_session():
    """
    Contextmanager that will create and teardown a session.
    """
    session = landsat_db.session_factory
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def provide_session(func):
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



@provide_session
def checkdb(session=None):
    """
    Checks if the database works.
    :param session: session of the sqlalchemy
    """
    session.execute('select 1 as is_alive;')
    print("select 1 as is_alive")


def props(obj):
    """
    将对象转化为 dict，去除 '__' 和 '_' 开头的属性
    :param obj: 
    :return: 
    """
    pr = {}
    for name in dir(obj):
        value = getattr(obj, name)
        if not name.startswith('__') \
                and not name.startswith('_') \
                and not callable(value):
            pr[name] = value
    return pr


def pendulum_convert_datetime(p_datetime):
    """
    将 pendulum 对象转化为 datetime 对象
    :param p_datetime: 
    :return: 
    """
    d_datetime = datetime.fromtimestamp(p_datetime.timestamp())

    return d_datetime


def datetime_convert_pendulum(d_datetime):
    """
    将 datetime 对象转化为  pendulum 对象
    :param d_datetime: 
    :return: 
    """
    p_datetime = pendulum.from_timestamp(d_datetime.timestamp())

    return p_datetime



if __name__ == '__main__':
    from time import sleep
    while(1):
        checkdb()
        sleep(60)

