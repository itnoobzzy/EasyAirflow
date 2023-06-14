#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:lichen
# datetime:2019/12/26 3:18 PM
import socket
import os
from typing import Optional
import logging
import loguru
from opencensus.trace import config_integration
from rest_api.akcensus.census import \
    InterceptTraceHandler, \
    TraceInterceptLogger, \
    TraceRootLogger
from rest_api.akcensus.uru import get_logger
from rest_api.akcensus.config import OPENCENSUS_EXT, OPENCENSUS_CURRENT_FRAME_DEPTH


def setup(env, current_frame_depth: Optional[int] = None, log_path: Optional[str] = None):
    is_debug = int(os.environ.get(f'{env.upper()}_DEBUG', 0))

    # # 1 重写系统根日志

    # ! RecursionError: maximum recursion depth
    # < 使用系统环境变量来实现TraceInterceptLogger和TraceRootLogger的切换
    root = TraceRootLogger()
    logging.Logger.root = root
    logging.root = root
    logging.Logger.manager = logging.Manager(logging.Logger.root)
    logging.setLoggerClass(TraceInterceptLogger)

    # # 2 添加拦截handler到原生日志
    if current_frame_depth:
        InterceptTraceHandler.current_frame_depth = current_frame_depth
    else:
        InterceptTraceHandler.current_frame_depth = OPENCENSUS_CURRENT_FRAME_DEPTH
    InterceptTraceHandler.is_debug = is_debug
    InterceptTraceHandler.logger = get_logger(is_debug=is_debug, log_path=log_path)

    logging.Logger.root.handlers = []
    InterceptTraceHandler.logger = InterceptTraceHandler.logger.bind(service=env, hostname=socket.gethostname())
    logging.Logger.root.addHandler(logging.NullHandler())
    logging.Logger.root.addHandler(InterceptTraceHandler())
    for k, v in logging.Logger.manager.loggerDict.items():
        if hasattr(v, 'addHandler'):
            setattr(v, 'handlers', list())
            v.addHandler(InterceptTraceHandler())

    # # 3 配置uru日志
    loguru.logger.remove()

    # # 4 patch需要追踪的库
    config_integration.trace_integrations(OPENCENSUS_EXT)  # < celery
