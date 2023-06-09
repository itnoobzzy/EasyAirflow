#!/usr/bin/env python
# -*- coding:utf-8 -*-

from airflow.models import DagRun
from airflow.utils.session import provide_session, NEW_SESSION
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import State
from sqlalchemy.orm.session import Session
from sqlalchemy import and_, func


class EasyAirflowTaskInstance(TaskInstance):

    @classmethod
    @provide_session
    def get_ti_states_summary(cls,
                              dag_id,
                              task_ids=None,
                              min_data_interval_end=None,
                              max_data_interval_end=None,
                              states=State.task_states,
                              session: Session = NEW_SESSION
                              ):
        """
        查询任务实例状态信息
        :param dag_id: 所属 dag_id
        :param task_ids: 需要查询的任务实例，为空查询对应 dag 下所有的任务实例状态
        :param min_data_interval_end: 查询范围的开始查询时间
        :param max_data_interval_end: 查询范围的结束查询时间
        :param states: 状态
        :return:
        """
        base_filter = and_(
            TaskInstance.dag_id == dag_id,
            TaskInstance.state.in_(states),
            TaskInstance.task_id.in_(task_ids) if task_ids else True,
            DagRun.data_interval_end >= min_data_interval_end if min_data_interval_end else True,
            DagRun.data_interval_end <= max_data_interval_end if max_data_interval_end else True
        )
        states_summary = session.query(TaskInstance.state, func.count(TaskInstance.state))\
            .join(DagRun, TaskInstance.run_id == DagRun.run_id)\
            .filter(base_filter)\
            .group_by(TaskInstance.state).all()
        return states_summary

    @classmethod
    @provide_session
    def get_ti_list(cls,
                    dag_id,
                    task_ids=None,
                    min_data_interval_end=None,
                    max_data_interval_end=None,
                    page_num=1,
                    page_size=10,
                    sort_field="data_interval_end",
                    direction=0,
                    states=State.task_states,
                    session: Session = NEW_SESSION
                    ):
        """
        查询任务实例列表信息
        :param dag_id: 所属 dag_id
        :param task_ids: 需要查询的任务实例，为空查询对应 dag 下所有的任务实例状态
        :param min_data_interval_end: 查询范围的开始查询时间
        :param max_data_interval_end: 查询范围的结束查询时间
        :param states: 状态
        :return:
        """
        base_filter = and_(
            TaskInstance.dag_id == dag_id,
            TaskInstance.state.in_(states),
            TaskInstance.task_id.in_(task_ids) if task_ids else True,
            DagRun.data_interval_end >= min_data_interval_end if min_data_interval_end else True,
            DagRun.data_interval_end <= max_data_interval_end if max_data_interval_end else True
        )
        count = session.query(func.count(1)).filter(base_filter).scalar()
        tis = session.query(TaskInstance, DagRun).filter(base_filter). \
            order_by(getattr(DagRun, sort_field).desc() if direction else getattr(DagRun, sort_field)) \
            .offset((page_num - 1) * page_size).limit(page_size).all()
        return count, tis

    @classmethod
    @provide_session
    def get_ti_by_id(cls,
               dag_id,
               task_id,
               data_interval_end=None,
               session: Session = NEW_SESSION):
        """
        查询任务实例
        :param dag_id:
        :param task_id:
        :param data_interval_end:
        :return:
        """
        ti = session.query(TaskInstance).join(DagRun, DagRun.run_id == TaskInstance.run_id).filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.task_id == task_id,
            DagRun.data_interval_end == data_interval_end).first()
        return ti

    @classmethod
    @provide_session
    def shutdown_ti(cls,
                    task_instance: TaskInstance,
                    ti_state: str,
                    session: Session = NEW_SESSION):
        """
        停止掉任务实例以及对应的 job
        :param task_instance:
        :param ti_state: ti new state
        :param session:
        :return:
        """
        from airflow.jobs.job import Job
        from airflow.models.taskreschedule import TaskReschedule

        if task_instance.state == State.RUNNING:
            task_instance.state = State.SHUTDOWN
        else:
            task_instance.state = ti_state
        session.merge(task_instance)

        tr = session.query(TaskReschedule).filter(
            TaskReschedule.dag_id == task_instance.dag_id,
            TaskReschedule.task_id == task_instance.task_id,
            TaskReschedule.execution_date == task_instance.execution_date,
            TaskReschedule.try_number == task_instance._try_number
        ).all()
        for t in tr:
            session.delete(t)

        if task_instance.job_id:
            jobs = session.query(Job).filter(Job.id == task_instance.job_id).all()
            for job in jobs:
                job.state = State.SHUTDOWN
                session.merge(job)
        session.commit()

