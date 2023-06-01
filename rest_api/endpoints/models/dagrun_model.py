#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:wanglong

from sqlalchemy import (Column,
                        Integer, String, Boolean,
                        DateTime, PickleType, and_, or_, tuple_, func)

from endpoints.models.dagrun_next_model import DagRunNext
from endpoints.models.task_instance_model import TaskInstance
from endpoints.models.taskinstance_next_model import TaskInstanceNext
from utils.airflow_database import Base, airflow_provide_session
from utils.state import State
from utils.times import dt2ts


class DagRun(Base):
    """
    DagRun describes an instance of a Dag. It can be created
    by the scheduler (for regular runs) or by an external trigger
    """
    __table_args__ = {'extend_existing': True}

    __tablename__ = "dag_run"

    ID_PREFIX = 'scheduled__'
    ID_FORMAT_PREFIX = ID_PREFIX + '{0}'

    id = Column(Integer, primary_key=True)
    dag_id = Column(String(250))
    execution_date = Column(DateTime)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    state = Column('state', String(50))
    run_id = Column(String(250))
    external_trigger = Column(Boolean, default=True)
    conf = Column(PickleType)

    # dag = None

    def props(self):

        name_list = {
            'dag_id', 'execution_date', 'start_date', 'end_date', 'state','_state'
            'run_id', 'external_trigger', 'id', 'conf'

        }

        date_list = {
            'start_date', 'end_date','execution_date'
        }

        float_list = {
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
        dag_id = pr['dag_id']
        execution_date = pr['execution_date']
        unique_key = '{}-{}'.format(dag_id, execution_date)

        pr['unique_key'] =unique_key
        return pr

    def unique_key(self):

        dict_info = self.props()

        dag_id = dict_info['dag_id']
        execution_date = dict_info['execution_date']

        unique_key = '{}-{}'.format(dag_id, execution_date)

        return unique_key


    @staticmethod
    @airflow_provide_session
    def get_dag_run_by_dag_id_execution_date(dag_id, execution_date, session=None):
        qry = session.query(DagRun)\
            .filter(DagRun.dag_id == dag_id) \
            .filter(DagRun.execution_date == execution_date)

        dagruns = qry.first()

        return dagruns

        pass

    @staticmethod
    @airflow_provide_session
    def get_dag_run(dag_id, limit_num, session=None):
        qry = session.query(DagRun).filter(
            DagRun.dag_id == dag_id) \
            .order_by(DagRun.execution_date.desc()) \
            .limit(limit_num)

        dagruns = qry.all()

        return dagruns

        pass

    @staticmethod
    @airflow_provide_session
    def get_dagrun_plan_execution_date(dag_id, limit_num, session=None):
        """
        通过 task instance  next 获取有哪些运行的 dag run 实例
        :param dag_id:
        :param limit_num:
        :param session:
        :return:
        """
        # qry = session.query(TaskInstanceNext.dag_id, TaskInstanceNext.next_execution_date,TaskInstanceNext.execution_date).filter(
        #     TaskInstanceNext.dag_id == dag_id
        # ).group_by(TaskInstanceNext.dag_id, TaskInstanceNext.next_execution_date,TaskInstanceNext.execution_date) \
        #     .order_by(TaskInstanceNext.next_execution_date.desc()) \
        #     .limit(limit_num)

        qry = session.query(DagRunNext.dag_id, DagRunNext.next_execution_date,DagRunNext.execution_date).filter(
            DagRunNext.dag_id == dag_id
        ).order_by(DagRunNext.next_execution_date.desc()) \
            .limit(limit_num)

        dagruns = qry.all()

        return dagruns

    @staticmethod
    @airflow_provide_session
    def get_dagrun_task_instances(dag_id, execution_date_timestamp_ms, session=None):
        task_instances = TaskInstance.get_task_instance_all_by_id(dag_id=dag_id, execution_date_timestamp_ms=execution_date_timestamp_ms)

        return task_instances
        pass

    @classmethod
    @airflow_provide_session
    def get_dag_run_with_next(cls,
                                    dag_ids,
                                    count_num=False,
                                    min_execution_date=None,
                                    max_execution_date=None,
                                    page_num=1,
                                    page_size=10,
                                    state=[],
                                    sort_field=None,
                                    direction=0,
                                    session=None):

        sort_field = sort_field or 'next_execution_date'

        if count_num is True:
            qry = session.query(func.count(DagRun.dag_id)) \
            .outerjoin(DagRunNext,
                       and_(DagRun.dag_id == DagRunNext.dag_id,
                            DagRun.execution_date == DagRunNext.execution_date))
        else:
            qry = session.query(DagRun, DagRunNext) \
                .outerjoin(DagRunNext,
                           and_(
                               DagRun.dag_id == DagRunNext.dag_id,
                               DagRun.execution_date == DagRunNext.execution_date))

        if min_execution_date is not None and max_execution_date is not None:
            qry = qry.filter(DagRunNext.next_execution_date.between(min_execution_date, max_execution_date))


        if state is not None and len(state) > 0:
            if State.NONE in state:
                qry = qry.filter(or_(DagRun.state.in_(state),
                                     DagRun.state == None))
            else:
                qry = qry.filter(DagRun.state.in_(state))

        if sort_field == 'next_execution_date':
            qry = qry.filter(DagRunNext.dag_id.in_(dag_ids))
        elif sort_field == 'start_date':
            qry = qry.filter(DagRun.dag_id.in_(dag_ids))

        if count_num is False:
            if sort_field == 'next_execution_date':
                if direction == 0:
                    qry = qry.order_by(DagRunNext.next_execution_date.asc())
                elif direction == 1:
                    qry = qry.order_by(DagRunNext.next_execution_date.desc())
            elif sort_field == 'start_date':
                if direction == 0:
                    qry = qry.order_by(DagRun.start_date.asc())
                elif direction == 1:
                    qry = qry.order_by(DagRun.start_date.desc())

            qry = qry.limit(page_size).offset((page_num - 1) * page_size)
            return qry.all()
        return qry.scalar()


    @classmethod
    @airflow_provide_session
    def get_dag_run_state_num_by_id(cls,
                                     id_list=None,
                                     id_date_list=None,
                                     state=None,
                                    count=False,
                                     session=None):
        qry = session.query(func.count(DagRun.dag_id))
        if id_list is not None:
            qry = qry.filter(DagRun.dag_id.in_(id_list))
        elif id_date_list is not None:
            qry = qry.filter(tuple_(DagRun.dag_id, DagRun.execution_date).in_(id_date_list))

        if count is True:
            return qry.scalar()


        if state is not None:
            if isinstance(state, list):
                if State.NONE in state:
                    qry = qry.filter(or_(DagRun.state.in_(state), DagRun.state == None))
                else:
                    qry = qry.filter(DagRun.state.in_(state))
            else:
                qry = qry.filter(DagRun.state == state)
        else:
            qry = qry.filter(DagRun.state == None)

        return qry.scalar()

    @classmethod
    @airflow_provide_session
    def get_dag_run_filter_execution_date(cls,
                                           dag_ids,
                                           min_execution_date=None,
                                           max_execution_date=None,
                                           session=None):
        qry = session.query(DagRunNext.dag_id, DagRunNext.execution_date) \
            .filter(DagRunNext.dag_id.in_(dag_ids))

        if min_execution_date is not None and max_execution_date is not None:
            qry = qry.filter(DagRunNext.next_execution_date.between(min_execution_date, max_execution_date))

        return qry.all()