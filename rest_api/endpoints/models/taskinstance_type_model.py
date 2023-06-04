#!/usr/bin/env python
# -*- coding:utf-8 -*-

import logging

from sqlalchemy import (Column,
                        Integer, String,
                        DateTime,)

from utils.airflow_database import Base
from utils.database import landsat_provide_session
from utils.times import datetime_timestamp_str

logger = logging.getLogger(__name__)


class TaskInstanceType(Base):
    __tablename__ = "task_instance_type"

    task_id = Column(String(250), primary_key=True, comment='task 名称')
    execution_date = Column(DateTime, primary_key=True, comment='执行时间')
    type = Column(Integer, primary_key=True, comment='触发类型 : 补数据触发 1 ')

    # 触发类型 : 补数据触发 1
    FILL_DATA_TYPE = 1
    # 周期类型，一般不出现在该表中
    CYCLE_TYPE = 2

    def __init__(self,
                 task_id=None
                 , execution_date=None
                 , type=FILL_DATA_TYPE
                 ):

        self.task_id = task_id
        self.execution_date = datetime_timestamp_str(execution_date)
        self.type = type

        pass

    def props(self):

        name_list = {
            'task_id', 'execution_date', 'type'
        }

        chinese_list = {
            'type'
        }

        pr = {}
        for name in dir(self):
            value = getattr(self, name)
            if name in chinese_list:
                if value == TaskInstanceType.FILL_DATA_TYPE:
                    pr[name] = '补数据'
            elif name in name_list:
                pr[name] = value

        # 获取对象的唯一主键
        task_id = pr['task_id']
        execution_date = pr['execution_date']
        unique_key = '{}-{}'.format(task_id, execution_date)

        pr['unique_key'] = unique_key
        return pr

    def unique_key(self):
        dict_info = self.props()
        task_id = dict_info['task_id']
        execution_date = dict_info['execution_date']
        unique_key = '{}-{}'.format(task_id, execution_date)
        return unique_key

    @landsat_provide_session
    def upsert_task_instance_type(self, session=None):
        """
        插入对象
        :param session:
        :return:
        """
        task_id = self.task_id
        execution_date = self.execution_date
        type = self.type
        qry = session.query(TaskInstanceType).filter(
            TaskInstanceType.task_id == task_id,
            TaskInstanceType.execution_date == execution_date,
            TaskInstanceType.type == type,
        )
        task_instance_type = qry.first()
        if task_instance_type is None:
            session.add(self)
        else:
            session.merge(self)
        session.commit()

    @classmethod
    @landsat_provide_session
    def get_all_complement_type(cls, session=None):
        """
        获取所 补类型的实例信息
        :param session:
        :return:
        """

        qry = session.query(TaskInstanceType).filter(
            TaskInstanceType.type == cls.FILL_DATA_TYPE
        )
        task_instance_types = qry.all()
        return task_instance_types

    @staticmethod
    @landsat_provide_session
    def get_task_all_instance(param, session=None):
        TI = TaskInstanceType
        qry = session.query(TI).filter(*param)
        all_data = qry.all()
        return all_data
