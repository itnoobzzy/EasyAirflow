#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:wanglong
from datetime import datetime

from airflow.utils.session import provide_session
from sqlalchemy import (Column,
                        String, DateTime)

from rest_api.utils.airflow_database import Base, airflow_provide_session
from rest_api.utils.times import dt2ts


class TaskInstanceNext(Base):
    """
    Task instances store the state of a task instance. This table is the
    authority and single source of truth around what tasks have run and the
    state they are in.

    The SqlAlchemy model doesn't have a SqlAlchemy foreign key to the task or
    dag model deliberately to have more control over transactions.

    Database transactions on this table should insure double triggers and
    any confusion around what task instances are or aren't ready to run
    even while multiple schedulers may be firing task instances.
    """

    __tablename__ = "task_instance_next"

    task_id = Column(String(250), primary_key=True)
    dag_id = Column(String(250), primary_key=True)
    execution_date = Column(DateTime, primary_key=True)
    next_execution_date = Column(DateTime, primary_key=True)

    def props(self):

        name_list = {
            'task_id', 'dag_id', 'execution_date', 'start_date', 'end_date', 'duration', 'state', 'try_number',
            'hostname',
            'unixname', 'job_id', 'pool', 'queue'
            , 'priority_weight', 'operator', 'queued_dttm', 'pid', 'max_tries'

        }

        date_list = {
            'start_date', 'end_date', 'execution_date','next_execution_date'
        }

        float_list = {
            'duration'
        }

        pr = {}
        for name in dir(self):
            value = getattr(self, name)

            if name in float_list:
                if value is not None:
                    pr[name] = int(value * 1000)
                else:
                    # pr[name] = value
                    pr[name] = 0

            elif name in date_list:
                if value is not None:
                    # pr[name] = int(value.timestamp() * 1000)
                    pr[name] = int(dt2ts(value) * 1000)
                else:
                    # pr[name] = value
                    pr[name] = 0
            elif name in name_list:
                pr[name] = value

        # 获取对象的唯一主键
        task_id = pr['task_id']
        execution_date = pr['execution_date']
        unique_key = '{}-{}'.format(task_id, execution_date)

        pr['unique_key'] =unique_key
        return pr

    def unique_key(self):

        dict_info = self.props()

        task_id = dict_info['task_id']
        execution_date = dict_info['execution_date']

        unique_key = '{}-{}'.format(task_id, execution_date)

        return unique_key



    def __init__(self, task, execution_date):
        self.dag_id = task.dag_id
        self.task_id = task.task_id
        self.execution_date = execution_date
        self.next_execution_date = task.dag.following_schedule(self.execution_date)

    @staticmethod
    @provide_session
    def get_task_instance_next( task_id, execution_date,session=None):
        """
        获取当前 task_instance 的 next_execution_date 计划时间
        :param dag_id: DAG ID
        :type dag_id: unicode
        :param task_id: task ID
        :type task_id: unicode
        :param execution_date: execution date
        :type execution_date: datetime
        :return: task_instance_next corresponding to the given dag_id and execution date
            if one exists. None otherwise.
        :rtype: airflow.models.TaskInstanceNext
        """
        qry = session.query(TaskInstanceNext).filter(
            # TaskInstanceNext.dag_id == dag_id,
            TaskInstanceNext.task_id == task_id,
            TaskInstanceNext.execution_date == execution_date,
        )
        return qry.first()


    @staticmethod
    @provide_session
    def get_task_instance_next_by_dag_plan( dag_id, next_execution_date,session=None):
        """
        获取当前 task_instance 的 next_execution_date 计划时间
        :param dag_id: DAG ID
        :type dag_id: unicode
        :param task_id: task ID
        :type task_id: unicode
        :param execution_date: execution date
        :type execution_date: datetime
        :return: task_instance_next corresponding to the given dag_id and execution date
            if one exists. None otherwise.
        :rtype: airflow.models.TaskInstanceNext
        """

        if isinstance(next_execution_date, int):
            next_execution_date = datetime.utcfromtimestamp(next_execution_date / 1000)

        qry = session.query(TaskInstanceNext).filter(
            TaskInstanceNext.dag_id == dag_id,
            # TaskInstanceNext.task_id == task_id,
            TaskInstanceNext.next_execution_date == next_execution_date,
        )
        return qry.first()

    @staticmethod
    @provide_session
    def get_task_instance_next_by_task_plan( task_id, next_execution_date,session=None):
        """
        获取当前 task_instance 的 next_execution_date 计划时间
        :param dag_id: DAG ID
        :type dag_id: unicode
        :param task_id: task ID
        :type task_id: unicode
        :param execution_date: execution date
        :type execution_date: datetime
        :return: task_instance_next corresponding to the given dag_id and execution date
            if one exists. None otherwise.
        :rtype: airflow.models.TaskInstanceNext
        """
        if isinstance(next_execution_date, int):
            next_execution_date = datetime.utcfromtimestamp(next_execution_date / 1000)

        qry = session.query(TaskInstanceNext).filter(
            # TaskInstanceNext.dag_id == dag_id,
            TaskInstanceNext.task_id == task_id,
            TaskInstanceNext.next_execution_date == next_execution_date,
        )
        return qry.first()

    @staticmethod
    @provide_session
    def get_task_all_instance(param, session=None):

        TI = TaskInstanceNext
        qry = session.query(TI).filter(*param)

        all_data = qry.all()

        return all_data

    @classmethod
    @provide_session
    def get_instance_filter_exectuion_date(cls,
                                           airflow_task_ids,
                                           min_execution_date=None,
                                           max_execution_date=None,
                                           session=None):
        qry = session.query(TaskInstanceNext.task_id, TaskInstanceNext.execution_date) \
            .filter(TaskInstanceNext.task_id.in_(airflow_task_ids))

        if min_execution_date is not None and max_execution_date is not None:
            qry = qry.filter(TaskInstanceNext.next_execution_date.between(min_execution_date, max_execution_date))

        return qry.all()
