#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:wanglong
import requests
import json
from sqlalchemy import (Column,Integer, String, DateTime, Float, func, and_, tuple_, or_)

from endpoints.models.taskinstance_next_model import TaskInstanceNext
from config import SERVE_LOG_PROT
from utils.airflow_database import Base, airflow_provide_session
from utils.state import State
from utils.times import dt2ts, datetime_timestamp_str, datetime_convert_pendulum


class TaskInstance(Base):
    __tablename__ = "task_instance"

    task_id = Column(String(250), primary_key=True)
    dag_id = Column(String(250), primary_key=True)
    execution_date = Column(DateTime, primary_key=True)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    duration = Column(Float)
    state = Column(String(20))
    _try_number = Column('try_number', Integer, default=0)
    max_tries = Column(Integer)
    hostname = Column(String(1000))
    unixname = Column(String(1000))
    job_id = Column(Integer)
    pool = Column(String(50), nullable=False)
    queue = Column(String(256))
    priority_weight = Column(Integer)
    operator = Column(String(1000))
    queued_dttm = Column(DateTime)
    pid = Column(Integer)
    executor_config = Column(String(250))

    def props(self):

        name_list = {
            'task_id', 'dag_id', 'execution_date', 'start_date', 'end_date', 'duration', 'state', 'try_number',
            'hostname',
            'unixname', 'job_id', 'pool', 'queue'
            , 'priority_weight', 'operator', 'queued_dttm', 'pid', 'max_tries'

        }

        date_list = {
            'start_date', 'end_date','execution_date','queued_dttm'
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

    @staticmethod
    @airflow_provide_session
    def get_task_instance(dag_id, task_id, execution_date, session=None):

        execution_date = datetime_timestamp_str(execution_date)

        TI = TaskInstance

        qry = session.query(TI).filter(
            TI.dag_id == dag_id,
            TI.task_id == task_id,
            TI.execution_date == execution_date)
        ti = qry.first()

        return ti

    @staticmethod
    @airflow_provide_session
    def get_task_instance_all(dag_id, task_id, session=None):

        TI = TaskInstance

        qry = session.query(TI).filter(
            # TI.dag_id == dag_id,
            TI.task_id == task_id
        )
        ti_list = qry.all()

        return ti_list

    @staticmethod
    @airflow_provide_session
    def get_task_instance_all_by_id(dag_id, execution_date_timestamp_ms,session=None):

        execution_date = datetime_timestamp_str(execution_date_timestamp_ms)
        TI = TaskInstance

        qry = session.query(TI).filter(
            TI.dag_id == dag_id,
            TI.execution_date == execution_date
        )
        ti_list = qry.all()

        return ti_list

    @staticmethod
    @airflow_provide_session
    def get_task_instance_all_by_task_ids( task_ids, session=None):

        TI = TaskInstance

        qry = session.query(TI).filter(
            TI.task_id.in_(task_ids)
        )
        ti_list = qry.all()

        return ti_list


    @staticmethod
    @airflow_provide_session
    def get_task_instances_date(dag_id, task_id, min_execution_date, max_execution_date, execution_num=None,
                                session=None):

        max_execution_date = datetime_timestamp_str(max_execution_date)
        min_execution_date = datetime_timestamp_str(min_execution_date)

        TI = TaskInstance

        qry = session.query(TI).filter(
            # TI.dag_id == dag_id,
            TI.task_id == task_id,
            TI.execution_date.between(min_execution_date, max_execution_date))

        if execution_num is None:

            tis = qry.all()

            return tis

        else:

            ti = qry.order_by(TI.execution_date).limit(1).offset(execution_num - 1).first()

            if ti is not None:
                return [ti]
            else:
                return []


    @airflow_provide_session
    def is_queued(self,session=None):
        if self.state in State.queued_state():
            return True
        else:
            return False

    @airflow_provide_session
    def is_success(self, session=None):
        if self.state in State.success_state():
            return True
        else:
            return False

    @airflow_provide_session
    def is_running(self, session=None):
        if self.state in State.running_state():
            return True
        else:
            return False

    @airflow_provide_session
    def is_failed(self, session=None):
        if self.state in State.failed_state():
            return True
        else:
            return False

    @airflow_provide_session
    def is_warn(self, session=None):
        if self.state in State.warn_state():
            return True
        else:
            return False

    @airflow_provide_session
    def mark_state(self, state, session=None):

        self.state = state

        session.merge(self)


    @airflow_provide_session
    def delete_state(self, session=None):

        session.delete(self)


    @airflow_provide_session
    def log_url(self, session=None):

        port = SERVE_LOG_PROT

        dag_id = self.dag_id
        task_id = self.task_id
        try_number = self._try_number
        hostname = self.hostname
        execution_date = self.execution_date

        execution_date_str = datetime_convert_pendulum(execution_date)

        filename_template = '{dag_id}/{task_id}/{execution_date}/{try_number}.log'
        log_relative_path = filename_template.format(dag_id=dag_id,
                                                     task_id=task_id,
                                                     execution_date=execution_date_str,
                                                     try_number=try_number)

        url = "http://{hostname}:{worker_log_server_port}/log/{log_relative_path}".format(
            hostname=hostname,
            worker_log_server_port=port,
            log_relative_path=log_relative_path
        )

        return url

    @staticmethod
    @airflow_provide_session
    def get_task_instance_avg(param, session=None):

        TI = TaskInstance
        ti_avg_duration_all = session.query(TI.task_id, func.avg(TI.duration)).filter(*param) \
            .group_by(TI.task_id) \
            .order_by(TI.task_id) \
            .all()

        return ti_avg_duration_all

    @staticmethod
    @airflow_provide_session
    def get_task_instance_page(param, page_size, current_page, session=None):

        TI = TaskInstance
        page_data = session.query(TI).filter(*param)\
            .order_by(TI.task_id)\
            .limit(page_size)\
            .offset((int(current_page) - 1) * page_size)

        return page_data

    @staticmethod
    @airflow_provide_session
    def get_task_all_instance(param, session=None):

        TI = TaskInstance
        qry = session.query(TI).filter(*param)

        all_data = qry.all()

        return all_data

    @staticmethod
    @airflow_provide_session
    def get_task_instance_sum(param, session=None):

        sum_num = session.query(TaskInstance) \
            .filter(*param).count()

        return sum_num

    @staticmethod
    @airflow_provide_session
    def get_task_instance_state_num(param, state, session=None):
        TI = TaskInstance
        qry = session.query(TI).filter(*param)

        from sqlalchemy import or_
        if isinstance(state, list):

            if State.NONE in state:
                qry = qry.filter(or_(TI.state.in_(state), TI.state == None))
            else:
                qry = qry.filter(TaskInstance.state.in_(state))

        else:
            qry = qry.filter(TaskInstance.state == state)

        state_num = qry.count()

        return state_num

    @staticmethod
    @airflow_provide_session
    def get_task_instance_by_param(param, session=None):
        TI = TaskInstance
        qry = session.query(TI).filter(*param).all()

        return qry

    @staticmethod
    def direct_run_task_instance(host, port, dag_id,task_id,execution_date):
        url = 'http://{host}:{port}/api/v1/task/instance/run'.format(host=host, port=port)

        values = {'dag_id': dag_id
                , 'task_id': task_id
                , 'execution_date':execution_date
                  }

        r = requests.post(url, data=values)

        if r.status_code != 200:
            # raise Exception("run task instance {} {} failed \n error:{}".format(task_id, execution_date, r.content))
            json_err = {"task_id": task_id,
                        "execution_date": str(execution_date),
                        "content": str(r.content),
                        "status": r.status_code}
            raise Exception(json.dumps(json_err))

    @classmethod
    def list_sort_compare(cls, l1, l2):
        """
        无序的排序，会改变原来集合的顺序
        :param l1:
        :param l2:
        :return:
        """
        l1.sort()
        l2.sort()

        if l1 == l2:
            return True
        else:
            return False

    @classmethod
    @airflow_provide_session
    def get_task_instance_with_next(cls,
                                    task_ids,
                                    count_num=False,
                                    min_execution_date=None,
                                    max_execution_date=None,
                                    page_num=1,
                                    page_size=10,
                                    state=[],
                                    sort_field=None,
                                    direction=0,
                                    instance_type=None,
                                    cycle_type=None,
                                    filldata_type=None,
                                    ti_type_tuple_list=[],
                                    session=None):

        if sort_field is None:
            sort_field = 'next_execution_date'

        if count_num is True:
            qry = session.query(func.count(TaskInstance.task_id)) \
            .outerjoin(TaskInstanceNext,
                       and_(TaskInstance.task_id == TaskInstanceNext.task_id,
                            TaskInstance.dag_id == TaskInstanceNext.dag_id,
                            TaskInstance.execution_date == TaskInstanceNext.execution_date))
        else:
            qry = session.query(TaskInstance, TaskInstanceNext) \
                .outerjoin(TaskInstanceNext,
                           and_(TaskInstance.task_id == TaskInstanceNext.task_id,
                                TaskInstance.dag_id == TaskInstanceNext.dag_id,
                                TaskInstance.execution_date == TaskInstanceNext.execution_date))

        if min_execution_date is not None and max_execution_date is not None:
            qry = qry.filter(TaskInstanceNext.next_execution_date.between(min_execution_date, max_execution_date))

        # 如果实例类型，只有 补数据  一种，那么就需要筛选出来，通过 task_id 和 execution_date 相同
        if cls.list_sort_compare(instance_type, [filldata_type]):
            qry = qry.filter(tuple_(TaskInstance.task_id, TaskInstance.execution_date).in_(ti_type_tuple_list))
        # 如果实例类型只有，周期性一种
        elif cls.list_sort_compare(instance_type, [cycle_type]):
            qry = qry.filter(tuple_(TaskInstance.task_id, TaskInstance.execution_date).notin_(ti_type_tuple_list))

        if state is not None and len(state) > 0:
            if State.NONE in state:
                qry = qry.filter(or_(TaskInstance.state.in_(state),
                                     TaskInstance.state == None))
            else:
                qry = qry.filter(TaskInstance.state.in_(state))

        if sort_field == 'next_execution_date':
            qry = qry.filter(TaskInstanceNext.task_id.in_(task_ids))
        elif sort_field == 'start_date':
            qry = qry.filter(TaskInstance.task_id.in_(task_ids))

        if count_num is False:
            if sort_field == 'next_execution_date':
                if direction == 0:
                    qry = qry.order_by(TaskInstanceNext.next_execution_date.asc())
                elif direction == 1:
                    qry = qry.order_by(TaskInstanceNext.next_execution_date.desc())
            elif sort_field == 'start_date':
                if direction == 0:
                    qry = qry.order_by(TaskInstance.start_date.asc())
                elif direction == 1:
                    qry = qry.order_by(TaskInstance.start_date.desc())

            qry = qry.limit(page_size).offset((page_num - 1) * page_size)
            return qry.all()
        return qry.scalar()

    @classmethod
    @airflow_provide_session
    def get_instance_state_num_by_id(cls,
                                     id_list=None,
                                     id_date_list=None,
                                     state=None,
                                     instance_type=None,
                                     cycle_type=None,
                                     filldata_type=None,
                                     ti_type_tuple_list=[],
                                     count=False,
                                     session=None):
        qry = session.query(func.count(TaskInstance.task_id))
        if id_list is not None:
            qry = qry.filter(TaskInstance.task_id.in_(id_list))
        elif id_date_list is not None:
            qry = qry.filter(tuple_(TaskInstance.task_id, TaskInstance.execution_date).in_(id_date_list))

        # 如果实例类型，只有 补数据  一种，那么就需要筛选出来，通过 task_id 和 execution_date 相同
        if cls.list_sort_compare(instance_type, [filldata_type]):
            qry = qry.filter(tuple_(TaskInstance.task_id, TaskInstance.execution_date).in_(ti_type_tuple_list))
        # 如果实例类型只有，周期性一种
        elif cls.list_sort_compare(instance_type, [cycle_type]):
            qry = qry.filter(tuple_(TaskInstance.task_id, TaskInstance.execution_date).notin_(ti_type_tuple_list))

        if count is True:
            return qry.scalar()


        if state is not None:
            if isinstance(state, list):
                if State.NONE in state:
                    qry = qry.filter(or_(TaskInstance.state.in_(state), TaskInstance.state == None))
                else:
                    qry = qry.filter(TaskInstance.state.in_(state))
            else:
                qry = qry.filter(TaskInstance.state == state)
        else:
            qry = qry.filter(TaskInstance.state == None)

        return qry.scalar()
