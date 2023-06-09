#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:wanglong

from __future__ import unicode_literals

from builtins import object


class Dependence(object):

    """
    1. 上游依赖事件 成功执行，失败不执行 1
    2. 上游依赖事件 成功不执行，失败执行 2
    3. 上游依赖事件 成功和失败，都执行 3
    """
    SUCCESS_RUN = 1
    FAILED_RUN = 2
    MUST_RUN = 3

    pass