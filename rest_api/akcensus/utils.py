#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:lichen
# datetime:2019/12/17 10:22 AM

import logging
import datetime
import pathlib2 as pathlib


def get_logger(name=None, level=5):
    from . import InterceptHandler, InterceptTraceHandler
    _logger = logging.getLogger(name)
    _logger.propagate = False
    _logger.setLevel(level)
    for h in _logger.handlers:
        if isinstance(h, InterceptTraceHandler) or isinstance(h, InterceptHandler):
            break
    else:
        _logger.addHandler(InterceptTraceHandler())
    return _logger


def serialize(obj, lables=None, ignore_lables=None, depth=4, ignore_p=True) -> dict:
    """
    返回一个obj的所有可序列化信息
    :param obj:
    :param lables:
    :param ignore_lables:
    :param depth:
    :param ignore_p: 忽略私有属性
    :return:
    """
    if not lables:
        is_all = True
    if not ignore_lables:
        ignore_lables = list()

    same_lables = set()

    def _(v, _deep):
        if _deep + 1 > depth:
            return str(v). \
                replace('\\', ''). \
                replace('\'', ''). \
                replace('\"', ''). \
                replace('\n', '\t')
        if isinstance(v, list) or isinstance(v, tuple) or isinstance(v, set):
            r = list()
            for i in v:
                r.append(_(i, _deep + 1))
            return r
        elif isinstance(v, str):
            return str(v)
        elif isinstance(v, dict):
            r = dict()
            for key, value in v.items():
                if key in ignore_lables:
                    continue
                if ignore_p and key.startswith('_'):
                    continue
                if is_all or key in lables:
                    r.update({key: _(value, _deep + 1)})
            return r
        elif isinstance(v, bool):
            return 1 if v else 0
        elif isinstance(v, int):
            return v
        elif isinstance(v, datetime.datetime):
            return int(v.timestamp()) * 1000000 + v.microsecond
        elif not v:
            return None
        else:
            if v in same_lables:
                return ''
            if not hasattr(v, '__dict__'):
                if hasattr(v, '__name__'):
                    return f'type:{v.__name__}'
                else:
                    return f'v'
            r = dict()
            for key, value in v.__dict__.items():
                if ignore_p and key.startswith('_') and key not in lables:
                    continue
                if is_all or key in lables:
                    r.update({key: _(value, _deep + 1)})
            same_lables.add(id(v))
            return r

    return _(obj, 0)


def get_path(path):
    path = pathlib.Path().joinpath(path)
    path.mkdir(exist_ok=True, parents=True)
    path = path.absolute().as_posix()
    return path
