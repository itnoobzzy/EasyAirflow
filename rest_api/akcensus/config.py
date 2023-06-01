#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:lichen
# datetime:2020/7/13 11:46 上午

import sys
import copy
import os
from .utils import get_path


# opencensus 配置

OPENCENSUS_EXT = ['requests', 'sqlalchemy']
OPENCENSUS_CURRENT_FRAME_DEPTH = os.environ.get('OPENCENSUS_CURRENT_FRAME_DEPTH', 2)

# loguru 配置

URU_RECORD_SPLIT = '|\001|'
URU_RECORD_BASE_FORMAT = [
    '{level.icon}',
    '<y>{time:YYYY-MM-DD HH:mm:ss.SS}</y>',
    '{process}:{thread}',
    '<y>{name}:{line}</y>',
    '<lvl>{name}:{module}:{function}</lvl>',
    '<lvl>{level}</lvl>',
    '{message}',
]
URU_RECORD_TRACE_FORMAT = [
    '{level.icon}',
    '<y>{time:YYYY-MM-DD HH:mm:ss.SS}</y>',
    '{name}:{line}',
    '<y>{extra[trace][trace_id]}</y>',
    '{extra[trace][parent_span_id]}',
    '<y>{extra[trace][span_id]}</y>',
    '{extra[trace][span_name]}',
    '<lvl>{level}</lvl>',
    '{message}',
]
URU_RECORD_EXTRA_FORMAT = ['{extra}']
URU_TRACE_LOG_NAME = 'tracer'

URU_BASE_LOG_CONFIG = {
    'format': URU_RECORD_SPLIT.join(URU_RECORD_TRACE_FORMAT),
    'colorize': False,
    'serialize': False,
    'backtrace': False,
    'diagnose': False,
    'enqueue': False,
    'catch': False,
    'filter': None,  # # callable
}

URU_SYS_LOG_CONFIG = copy.deepcopy(URU_BASE_LOG_CONFIG)
URU_SYS_LOG_CONFIG.update({
    'colorize': True
})

URU_FILE_LOG_CONFIG = copy.deepcopy(URU_BASE_LOG_CONFIG)
URU_FILE_LOG_CONFIG.update({
    'format': '{message}',
    'serialize': True,
    'enqueue': True,
    'rotation': '00:00',  # # 回转时间或者大小
    'retention': '3 days',  # # 保留
    'compression': None,  # # 压缩
    'delay': False,
    'buffering': 1,
    'encoding': 'utf-8',
})

# 过滤配置

# # 过滤指定模块，日志格式中 '{name}:{line}' 的name字段活着拼接字段
URU_FILTER_NAME_OR_LINE = [
    'sqlalchemy.engine.base',
    'sqlalchemy.orm.mapper',
    'sqlalchemy.orm.relationships',
    'sqlalchemy.orm.strategies',
    'sqlalchemy.orm.path_registry',  # # DEBUG
    'sqlalchemy.pool.base',  # # DEBUG
    'sqlalchemy.engine.result',  # # DEBUG
    'sqlalchemy.pool',  # # DEBUG
]
URU_FILTER_KEYWORDS = []
# # 不向上传播
_filter_propagate = lambda r, kwargs: r['level'].name == kwargs['level']
# # 过滤指定模块
_filter_name = lambda r, kwargs: \
    r['name'] not in URU_FILTER_NAME_OR_LINE and f'{r["name"]}:{r["line"]}' not in URU_FILTER_NAME_OR_LINE
# # 过滤关键词
_filter_keywords = lambda r, kwargs: sum([k in r['message'] for k in URU_FILTER_KEYWORDS]) == 0


# 日志配置

# # sys.stdout 标准输出到屏幕
# # file_path:str 输出到指定路径
# ! _开头的参数不是uru原生自带的
URU_FILE_PATH = get_path('./logs')
URU_LOGGERS = [
    {
        'args': [sys.stdout],
        'kwargs': {
            'level': 'INFO',
        },
        '_filter': [_filter_name, _filter_propagate],
        '_config': copy.deepcopy(URU_SYS_LOG_CONFIG),
    },
    {
        'args': [URU_FILE_PATH],
        'kwargs': {
            'level': 'INFO',
        },
        '_filter': [_filter_propagate, _filter_name],
        '_config': copy.deepcopy(URU_FILE_LOG_CONFIG),
    },
    {
        'args': [sys.stdout],
        'kwargs': {
            'level': 'TRACE',
        },
        '_filter': [_filter_propagate, _filter_keywords],
        '_config': copy.deepcopy(URU_SYS_LOG_CONFIG),
    },
    {
        'args': [URU_FILE_PATH],
        'kwargs': {
            'level': 'TRACE',
        },
        '_config': copy.deepcopy(URU_FILE_LOG_CONFIG),
        '_filter': [_filter_propagate, _filter_keywords],
    },
    {
        'args': [sys.stdout],
        'kwargs': {
            'level': 'WARNING',
        },
        '_filter': [_filter_propagate, _filter_keywords, _filter_name],
        '_config': copy.deepcopy(URU_SYS_LOG_CONFIG),
    },
    {
        'args': [URU_FILE_PATH],
        'kwargs': {
            'level': 'WARNING',
        },
        '_filter': [_filter_propagate, _filter_keywords, _filter_name],
        '_config': copy.deepcopy(URU_FILE_LOG_CONFIG),
    },
    {
        'args': [sys.stdout],
        'kwargs': {
            'level': 'ERROR',
            'backtrace': True,
            'diagnose': True,
        },
        '_filter': [_filter_propagate, _filter_keywords, _filter_name],
        '_config': copy.deepcopy(URU_SYS_LOG_CONFIG),
    },
    {
        'args': [URU_FILE_PATH],
        'kwargs': {
            'level': 'ERROR',
            'backtrace': True,
            'diagnose': True,
        },
        '_filter': [_filter_propagate, _filter_keywords, _filter_name],
        '_config': copy.deepcopy(URU_FILE_LOG_CONFIG),
    },
]
URU_DEBUG_LOGGER = [
    {
        'args': [sys.stdout],
        'kwargs': {
            'level': 'DEBUG',
        },
        '_filter': [_filter_propagate, _filter_keywords, _filter_name],
        '_config': copy.deepcopy(URU_SYS_LOG_CONFIG),
    }
]

