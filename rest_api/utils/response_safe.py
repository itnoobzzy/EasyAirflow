import functools
from datetime import datetime, timedelta
import json
import logging
import traceback

from flask import Response, jsonify

from utils.exceptions import ConnectionsException, DagsException

logger = logging.getLogger(__name__)

def json_format(o):
    """
    序列化数据
    :param o:
    :return:
    """
    if isinstance(o, datetime):
        return int(o.timestamp() * 1000)
    elif isinstance(o, list):
        o = [json_format(i) for i in o]
        return o
    elif isinstance(o, dict):
        r = list()
        for k, i in o.items():
            # if type(k) is str and k[0] is '_':
            #     continue f'\\001\'
            r.append((k, json_format(i)))
        return dict(r)
    else:
        return o


def safe(f):
    """
    统一返回Response格式的装饰器
    返回格式为一个状态码和数据字典，例:
    """
    def wraps(*args, **kwargs):
        try:
            resp = f(*args, **kwargs)
        except (ConnectionsException, DagsException) as e:
            logger.error(traceback.format_exc())
            return {'error': str(e), 'status': e.code, 'data': {}}
        except Exception as e:
            logger.error(traceback.format_exc())
            return {'error': str(e), 'status': 5001, 'data': {}}
        return resp
    return functools.update_wrapper(wraps, f)