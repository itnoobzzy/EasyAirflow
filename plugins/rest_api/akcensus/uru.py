#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
基于loguru进行扩展开发

level新增日志等级:
logger.level(name='CUSTOM', no=15, color='<blue>', icon='@')

add注册handler:
sink->filter->levelno->format->encoding
_log日志:
context->lazy->format()构造record->pathcer->emit
_handler:
levelno->filter->formater和_logformat有区别，构造Message对象->is_raw->serilize->self._sink.write调用add的自定义sink函数，传入Message对象

contextvar,extra={}->写入extra
lazy -> format 进log['message'],需要format=支持,message的forma在这里完成
pathcer ,patch 调用path()方法，修改log_record对象
handler,format，exception的format在这里完成，exception和error是同级信息

"""

from typing import List, Callable
from types import MethodType
import json

import loguru
from loguru._logger import Core, Logger
from loguru._handler import Handler

from rest_api.akcensus.config import *


def _serialize_record(cls, text, record):
    serializable = {

        'record': {
            'timestamp': int(record['time'].timestamp()*1000),
            'time': record['time'].strftime('%Y-%m-%d %H:%M:%S'),
            'exception': record['exception']
                         and {
                             'type': record['exception'].type.__name__,
                             'value': record['exception'].value,
                             'traceback': bool(record['exception'].traceback),
                         },
            'message': record['message'],
            'file': record['file'].name,
            'line': record['line'],
            'function': record['function'],
            'level': record['level'].name,
            'name': record['name'],
            'process': record['process'].id,
            'thread': record['thread'].id,
            'extra': {

            },
        },
        'trace': dict(
            service=record['extra'].pop('service'),
            hostname=record['extra'].pop('hostname'),
            **record['extra'].pop('trace'))
    }

    serializable['record']['extra'] = record['extra']

    return json.dumps(serializable, default=str, ensure_ascii=False) + "\n"  # ! ujson 没有default参数


# # path uru的序列化方法
Handler._serialize_record = MethodType(_serialize_record, Handler)


# # 过滤
def _filter(filters: List[Callable], **kwargs):
    def _(record):
        for f in filters:
            if not f(record, kwargs):
                return False
        else:
            return True

    return _


def get_logger(is_debug=False, log_path=None, ) -> loguru._logger.Logger:
    _logger = Logger(Core(), None, 0, False, False, False, False, True, None, {})

    if is_debug:
        loggers = URU_LOGGERS + URU_DEBUG_LOGGER
    else:
        loggers = URU_LOGGERS
    if log_path:
        log_path = get_path(log_path)
    else:
        log_path = URU_FILE_PATH

    for l in loggers:
        if isinstance(l['args'][0], str):
            l['args'][0] = f'{log_path}/{l["kwargs"]["level"].lower()}.log'
        l['_config'].update(l['kwargs'])
        l['kwargs'].update(l.pop('_config'))
        l['kwargs']['filter'] = _filter(l.pop('_filter'), **l['kwargs'])

        _logger.add(*l['args'], **l['kwargs'])

    return _logger
