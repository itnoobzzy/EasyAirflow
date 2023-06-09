#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:wanglong
from datetime import datetime

from sqlalchemy import Column, Integer, String, Text, Index, DateTime

from rest_api.utils.airflow_database import Base


class Log(Base):
    """
    Used to actively log events to the database
    """

    __tablename__ = "log"

    id = Column(Integer, primary_key=True)
    dttm = Column(DateTime)
    dag_id = Column(String(250))
    task_id = Column(String(250))
    event = Column(String(30))
    execution_date = Column(DateTime)
    owner = Column(String(500))
    extra = Column(Text)

    __table_args__ = (
        Index('idx_log_dag', dag_id),
    )

    def __init__(self, event, task_instance, owner=None, extra=None, **kwargs):
        self.dttm = datetime.utcnow()
        self.event = event
        self.extra = extra

        task_owner = None

        if task_instance:
            self.dag_id = task_instance.dag_id
            self.task_id = task_instance.task_id
            self.execution_date = task_instance.execution_date
            task_owner = task_instance.task.owner

        if 'task_id' in kwargs:
            self.task_id = kwargs['task_id']
        if 'dag_id' in kwargs:
            self.dag_id = kwargs['dag_id']
        if 'execution_date' in kwargs:
            if kwargs['execution_date']:
                self.execution_date = kwargs['execution_date']

        self.owner = owner or task_owner
