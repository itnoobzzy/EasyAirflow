#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:lichen
# datetime:2019/12/16 2:34 PM

"""
- 改造原生logging功能，并使用loguru功能
- 接入opencensus功能到支持的库
使用：
在主进程装载完成，启动之前调用
配置环境变量{env.upper()}_DEBUG，启用DEBUG信息
重写config.URU_FILTER_NAME_OR_LINE来过滤日志

``` python
try:
    import akcensus
    akcensus.setup('landsat', current_frame_depth=2,log_path='./logs')
    akcensus.URU_FILTER_NAME_OR_LINE=[]
except Exception as e:
    logging.error(str(e))
```

"""

import pathlib
import sys

sys.path.append(pathlib.Path().absolute().as_posix())


from rest_api.akcensus.core import setup
from rest_api.akcensus.census import tracer
from rest_api.akcensus.utils import serialize, get_path, get_logger
from rest_api.akcensus.config import URU_FILTER_NAME_OR_LINE, URU_FILTER_KEYWORDS

# ~~支持多个export的export~~
# ~~优化serialize对相同对象的过滤，提高效率~~
# < todo 支持自定义选项控制是否输出到屏幕
# ~~新增handlers,1，分离出专门用于埋点的handler 2，新建专门用于发送异步任务的handler（暂定celery）~~
# < todo 日志结构体，事件消息结构体
# ~~superset打印出来的堆栈有问题，没有定位到logger.info的位置~~
# ~~增加更多tracer对象信息到日志~~
# ~~ 调整日志结构划分为系统内，系统外。系统内包括record和埋点，系统外是trace~~
# ~~ 优化tracer的使用，避免硬编码~~
