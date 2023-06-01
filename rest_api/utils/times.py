#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:wanglong

from datetime import datetime, timedelta

import pendulum
import time
import calendar

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


def datetime_convert_pendulum_utc8(d_datetime):
    """
    将 datetime 对象转化为  pendulum 对象
    :param d_datetime:
    :return:
    """
    new_d_datetime = d_datetime + timedelta(hours=8)
    p_datetime = pendulum.from_timestamp(new_d_datetime.timestamp())

    return p_datetime


def datetime_convert_pendulum_by_timezone(d_datetime):

    offset_utc_seconds = -time.timezone
    # utc 0
    if offset_utc_seconds == 0:
        execution_date_str = datetime_convert_pendulum(d_datetime)
    # utc 8
    elif offset_utc_seconds == 28800:
        execution_date_str = datetime_convert_pendulum_utc8(d_datetime)
    else:
        execution_date_str = datetime_convert_pendulum(d_datetime)

    return execution_date_str


def datetime_timestamp_str(execution_date):
    """
    时间戳，本身与 时区没有关系
    但是 datetime 与时区有关系，所以，datetime 和本地时区有关
    将 时间戳 转为 datetime 格式 ，datetime 默认为 格式，
    统一 0 时区
    :param execution_date:
    :return:
    """
    # 注意时区问题

    offset_utc_seconds = -time.timezone

    if isinstance(execution_date, int):

        # execution_date = datetime.fromtimestamp(execution_date / 1000.0 - offset_utc_seconds).strftime('%Y-%m-%d %H:%M:%S.%f')
        execution_date = datetime.utcfromtimestamp(execution_date / 1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')


    return execution_date




def dt2ts(dt):
    """Converts a datetime object to UTC timestamp

    naive datetime will be considered UTC.

    """

    return calendar.timegm(dt.utctimetuple())