#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:lichen
# datetime:2019/12/17 10:37 AM

import logging
from typing import Optional, List, AnyStr, Set

import loguru
import json
import datetime

from opencensus.trace import tracer, samplers
from opencensus.trace import logging_exporter, print_exporter, file_exporter
from opencensus.log import TraceLogger
from opencensus.trace import base_exporter
from opencensus.common.transports import sync

from rest_api.akcensus.utils import serialize
from rest_api.akcensus.config import URU_RECORD_SPLIT


class NullExporter(base_exporter.Exporter):
    """
    不输出任何内容
    """

    def __init__(self, transport=sync.SyncTransport):
        self.transport = transport(self)

    def emit(self, span_data):
        pass

    def export(self, span_data):
        self.transport.export(span_data)


class MultiExporter(base_exporter.Exporter):

    def __init__(self, exports: List[base_exporter.Exporter], transport=sync.SyncTransport):
        self.transport = transport(self)
        self.exports = exports

    def emit(self, span_data):
        for export in self.exports:
            export.emit(span_data)

    def export(self, span_data):
        self.transport.export(span_data)


class InterceptHandler(logging.Handler):
    """
    不做oc追踪的拦截器
    """
    logger: Optional[loguru._Logger] = None
    current_frame_depth = 1

    def get_log_level(self, record):
        # Get corresponding Loguru level if it exists
        try:
            level = loguru.logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        return level

    def get_frame_depth(self):
        # Find caller from where originated the logged message
        frame, depth = logging.currentframe(), self.current_frame_depth  # # 注意depth的初始值
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1
        return depth

    def emit(self, record):

        level = self.get_log_level(record)
        depth = self.get_frame_depth()
        self._uru_log(depth, level, record)

    def _uru_log(self, depth, level, record):
        with self.logger.contextualize(**record.extra):
            self.logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


_NullHandler = logging.NullHandler()

tracer = tracer.Tracer(
    sampler=samplers.AlwaysOnSampler(),
    # exporter=logging_exporter.LoggingExporter(handler=_NullHandler)
    # exporter=print_exporter.PrintExporter(),
    # exporter=file_exporter.FileExporter('tracer.log')
    exporter=NullExporter()
)


class InterceptTraceHandler(InterceptHandler):
    """
    要注意，exporter可能会带来RecursionError: maximum recursion depth错误
    """

    def _uru_log(self, depth, level, record):
        """
        :param depth:
        :param level:
        :param record:
        :return:
        """

        span = tracer.current_span()

        with self.logger.contextualize(
                trace={
                    'span_name': span.name if span else None,
                    'span_id': span.span_id if span else '0' * 16,
                    'trace_id': span.context_tracer.trace_id if span else tracer.tracer.trace_id,
                    'parent_span_id': span.parent_span.span_id if span else None
                },
                **record.extra,
        ):
            if record.name == 'tracer':
                self.logger.opt(depth=depth, exception=record.exc_info).trace(record.getMessage())
            else:
                self.logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


class CeleryHandler(InterceptHandler):
    terminated_by = URU_RECORD_SPLIT
    frame_depth = 30

    def __init__(self,
                 broker: AnyStr,
                 backend: AnyStr,
                 catalog: Set,
                 task: AnyStr,
                 instance=None,
                 debug: bool = False,
                 *args, **kwargs):
        super(CeleryHandler, self).__init__(*args, **kwargs)
        import celery
        self.instance = instance
        self.catalog = catalog
        self.task = task
        self.celery = celery.Celery(broker=broker, backend=backend)
        self.celery.conf['broker_connection_timeout'] = 1
        self.celery.conf['redis_socket_timeout'] = 1  # # 如果失败不能影响业务
        self.celery.conf['redis_socket_connect_timeout'] = 1
        if debug:
            logging.root.level = 10

    def _get_ti(self):
        frame, depth = logging.currentframe(), 0
        while depth <= self.frame_depth:
            frame = frame.f_back
            depth += 1
            if not frame:
                return None
            for k, v in frame.f_locals.__dict__.items():
                if isinstance(frame.f_locals[k], self.instance):
                    return frame.f_locals[k]

    def emit(self, record):
        try:
            if record.name not in self.catalog or self.terminated_by in record.msg:
                return
            instance = self._get_ti() if self.instance else dict()

            instance = {k: v.timestamp() if isinstance(v, datetime.datetime) else str(v) for k, v in
                        instance.__dict__.items()}
            _record = {k: str(v) for k, v in record.__dict__.items()}
            logging.debug('{}{}'.format(self.terminated_by, json.dumps(_record)))
            logging.debug('{}{}'.format(self.terminated_by, json.dumps(ti)))
            self.celery.send_task(
                self.task,
                kwargs={"record": record.__dict__, 'instance': instance},
            )
        except Exception as e:
            logging.error('{}{}'.format(self.terminated_by, str(e)))


class TraceInterceptLogger(TraceLogger):

    def __init__(self, *args, **kwargs):
        super(TraceInterceptLogger, self).__init__(*args, **kwargs)
        self.addHandler(InterceptTraceHandler())
        self.propagate = False

    def makeRecord(self, *args, **kwargs):

        extra = args[8]
        args = tuple(list(args[:8]) + [{}] + list(args[9:]))
        record = super(TraceInterceptLogger, self).makeRecord(*args, **kwargs)
        if extra:
            record.__dict__['extra'] = serialize(extra, depth=3)
        else:
            record.__dict__['extra'] = dict()
        return record


class TraceRootLogger(logging.RootLogger, TraceInterceptLogger):

    def __init__(self, level=0):
        super(TraceRootLogger, self).__init__(level)
        self.addHandler(InterceptTraceHandler())  # ! RecursionError: maximum recursion depth
        self.propagate = True
