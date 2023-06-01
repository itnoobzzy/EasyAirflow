#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:wanglong

from sqlalchemy import (Column,
                        Integer, String, DateTime)

from utils.database import Base


class TaskReschedule(Base):
    """
    TaskReschedule tracks rescheduled task instances.
    """

    __tablename__ = "task_reschedule"

    id = Column(Integer, primary_key=True)
    task_id = Column(String(250), nullable=False)
    dag_id = Column(String(250), nullable=False)
    execution_date = Column(DateTime, nullable=False)
    try_number = Column(Integer, nullable=False)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)
    duration = Column(Integer, nullable=False)
    reschedule_date = Column(DateTime, nullable=False)
