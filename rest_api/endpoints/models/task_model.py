#!/usr/bin/env python
# -*- coding:utf-8 -*-

import logging
from datetime import datetime, timedelta

from croniter import croniter
from sqlalchemy import (Column,
                        Integer, String, Boolean,
                        DateTime, JSON)

from endpoints.models.base_model import DeletionMixin, AuditMixinNullable
from endpoints.models.dag_task_dep_model import DagTaskDependence
from utils.database import Base, landsat_provide_session
from utils.dependence import Dependence

logger = logging.getLogger(__name__)


class TaskDefine(Base, DeletionMixin, AuditMixinNullable):
    __tablename__ = 'l_task_define'

    id = Column(Integer, primary_key=True)
    task_id = Column(String(250), unique=True, comment='task 名称, 唯一')
    dag_id = Column(String(250), comment='dag 名称, 全局唯一')
    owner = Column(String(250), comment='责任人名称')
    is_publish = Column(Boolean, default=False, comment='是否发布')
    operator = Column(String(100), nullable=True, comment='任务类型')
    describe = Column(String(1000), nullable=True, comment='任务描述')
    email = Column(String(500), nullable=True, comment='邮件发送人')
    email_on_retry = Column(Boolean, default=False, comment='重试是否发送邮件')
    email_on_failure = Column(Boolean, default=True, comment='失败是否发送邮件')
    start_date = Column(DateTime, nullable=True, comment='任务发布后开始时间')
    end_date = Column(DateTime, nullable=True, comment='任务发布后结束时间')
    trigger_rule = Column(String(50), nullable=True, comment='触发条件 all_success')
    depends_on_past = Column(Boolean, default=False, comment='是否依赖上一个运行周期的结束')
    wait_for_downstream = Column(Boolean, default=False, comment='是否等待上一个周期，下游任务完成')
    schedule_interval = Column(String(1024), nullable=True, comment='调度周期，与 cron 格式一致，但是没有秒')
    retries = Column(Integer, nullable=True, comment='重试次数')
    retry_delay_num_minutes = Column(Integer, nullable=True, comment='重试延迟时间，单位分钟')
    execution_timeout_num_minutes = Column(Integer, nullable=True, comment='超时时间，单位分钟')
    pool = Column(String(50), nullable=True, comment='运行的任务池')
    queue = Column(String(256), nullable=True, comment='运行任务的队列')
    priority_weight = Column(Integer, nullable=True, comment='权重')
    private_params = Column(JSON, nullable=True, comment='私有参数,比如 sql')

    def __repr__(self):
        return self.task_id

    @classmethod
    def get_key_name_list(cls):
        name_list = {'owner', 'id', 'ctime', 'mtime', 'deletion', 'task_id', 'dag_id', 'is_publish', 'operator',
                     'describe', 'email', 'email_on_retry', 'email_on_failure', 'start_date', 'end_date', 'trigger_rule',
                     'depends_on_past', 'wait_for_downstream', 'schedule_interval', 'retries', 'retry_delay_num_minutes', 'execution_timeout_num_minutes', 'pool', 'queue', 'priority_weight',
                     'private_params', 'created_by_fk', 'changed_by_fk'}

        return name_list

    def props(self):
        name_list = TaskDefine.get_key_name_list()
        pr = {}
        for name in dir(self):
            value = getattr(self, name)
            if name in name_list:
                pr[name] = value
        return pr

    def __init__(self,
                 task_id=None
                 , start_date=None
                 , end_date=None
                 , owner=None
                 , schedule_interval=None
                 , operator=None
                 , describe=None
                 , private_params=None
                 , pool='default_pool'
                 , queue='default'
                 , trigger_rule='all_success'
                 , priority_weight=1
                 , retries=2
                 , retry_delay_num_minutes=30
                 , execution_timeout_num_minutes=120
                 , dag_id=None
                 , is_publish=None
                 , email=None
                 , email_on_retry=None
                 , email_on_failure=None
                 , depends_on_past=None
                 , wait_for_downstream=None
                 ):

        self.task_id = task_id
        self.start_date = start_date
        self.end_date = end_date
        self.owner = owner

        self.schedule_interval = schedule_interval
        self.operator = operator
        self.describe = describe
        self.private_params = private_params
        self.pool = pool
        self.queue = queue
        self.trigger_rule = trigger_rule
        self.priority_weight = priority_weight
        self.retries = retries
        self.retry_delay_num_minutes = retry_delay_num_minutes
        self.execution_timeout_num_minutes = execution_timeout_num_minutes
        self.dag_id = dag_id
        self.is_publish = is_publish

        self.email = email
        self.email_on_retry = email_on_retry
        self.email_on_failure = email_on_failure

        self.depends_on_past = depends_on_past
        self.wait_for_downstream = wait_for_downstream

    @staticmethod
    @landsat_provide_session
    def get_primary_id(task_id, session=None):
        task = TaskDefine.get_task(task_id, session=session)
        if task is not None:
            return task.id
        else:
            return None

    @landsat_provide_session
    def upsert(self, session=None):
        task_id = self.task_id
        task_primary_id = TaskDefine.get_primary_id(task_id, session=session)
        if task_primary_id:
            self.id = task_primary_id
            session.merge(self)
        else:
            session.add(self)
        session.commit()

    @staticmethod
    @landsat_provide_session
    def get_tasks_by_dag_id(dag_id, session=None):
        """
        获取指定 dag 下的所有任务
        :param dag_id:
        :param session:
        :return:
        """
        qry = session.query(TaskDefine).filter(
            TaskDefine.dag_id == dag_id,
        )
        tasks = qry.all()
        return tasks

    @staticmethod
    @landsat_provide_session
    def get_task(id_or_task_id, session=None):
        """
        task_id 为 dag 的 task_id
        :param id_or_task_id:
        :param session:
        :return:
        """
        if isinstance(id_or_task_id, str):
            qry = session.query(TaskDefine).filter(
                TaskDefine.task_id == id_or_task_id,
            )
        else:
            qry = session.query(TaskDefine).filter(
                TaskDefine.id == id_or_task_id,
            )
        task = qry.first()
        return task

    @staticmethod
    def external_task_id(task_id, upstream_up_id):
        return '{}_wait_{}'.format(task_id, upstream_up_id)

    @staticmethod
    @landsat_provide_session
    def cancel_publish_task(id_or_task_id, session=None):
        """
        下线任务，并且下线任务依赖的影子任务
        :param id_or_task_id:
        :param session:
        :return:
        """
        task = TaskDefine.get_task(id_or_task_id)
        task.is_publish = False
        session.merge(task)

        task_id = task.task_id
        upstream_task_ids = DagTaskDependence.get_task_up_depends(task_id)
        for upstream_task_id in upstream_task_ids:
            external_task_id = TaskDefine.external_task_id(task_id, upstream_task_id)
            external_task = TaskDefine.get_task(external_task_id)
            if external_task is not None:
                external_task.is_publish = False
                session.merge(external_task)

    @staticmethod
    def timestamp_no_second(timestamp_ms):
        """
        去掉所有的 0 秒
        :param timestamp_ms:
        :return:
        """
        datetime_has_second = datetime.fromtimestamp(int(timestamp_ms / 1000))
        datetime_zero_second = datetime_has_second - timedelta(seconds=(datetime_has_second.second))
        timestamp_no_second = datetime_zero_second.timestamp()
        return timestamp_no_second

    @staticmethod
    def is_cron_point(cron_str, time_datetime):
        iter_corn = croniter(cron_str, time_datetime)
        now_datetime = iter_corn.get_next(datetime)
        if time_datetime == now_datetime:
            return True
        else:
            return False

    @staticmethod
    def set_task_start_end_datetime(start_timestamp_ms, end_timestamp_ms, cron_str):

        start_timestamp_s = TaskDefine.timestamp_no_second(start_timestamp_ms)
        end_timestamp_s = TaskDefine.timestamp_no_second(end_timestamp_ms)

        def get_prev_datetime(cron_str, timestamp_s):
            time_datetime = datetime.utcfromtimestamp(timestamp_s)
            iter_corn = croniter(cron_str, time_datetime)
            # 当前时间的前一个周期
            pre_datetime = iter_corn.get_prev(datetime)
            if not TaskDefine.is_cron_point(cron_str, time_datetime):
                # 如果，恰好不是调度点，减去 1 秒，确保不越界
                pre_datetime = pre_datetime - timedelta(seconds=1)
            return pre_datetime

        start_pre_datetime = get_prev_datetime(cron_str, start_timestamp_s)
        end_pre_datetime = get_prev_datetime(cron_str, end_timestamp_s)

        return start_pre_datetime, end_pre_datetime

    @staticmethod
    @landsat_provide_session
    def task_add_dep_by_user_dep(task, session=None):
        """
        添加任务间依赖关系
        :param task:
        :param session:
        :return:
        """
        task_id = task.task_id
        dag_id = task.dag_id
        upstream_task_ids = DagTaskDependence.get_task_up_depends(task_id)

        for upstream_task_id in upstream_task_ids:
            upstream_task = TaskDefine.get_task(upstream_task_id)
            """ 
            1. 上游依赖事件 成功执行，失败不执行 1
            2. 上游依赖事件 成功不执行，失败执行 2
            3. 上游依赖事件 成功和失败，都执行 3

            如果是 1 ，那么直接使用 dag 的内部依赖
            如果是 2 和 3，就不能使用 dag 的内部依赖，
            task 依赖 额外依赖，额外依赖 上游任务
            dag_id 不一致，就是依赖影子任务
            """
            # 要通过查表来获取依赖类型
            depend_task = DagTaskDependence.get_task_depends_by_type(task_id, upstream_task_id)
            depend_status = depend_task.trigger_rule

            if upstream_task.dag_id == task.dag_id and depend_status == Dependence.SUCCESS_RUN:
                # 添加 dag 内的依赖
                upstream_task_id = upstream_task.task_id
                downstream_task_id = task_id
                DagTaskDependence.add_airflow_task_dep(dag_id, downstream_task_id, upstream_task_id)
            elif upstream_task.dag_id == task.dag_id:
                # 添加 dag 内的依赖，但是需要额外依赖维护，依赖关系
                external_task = TaskDefine.add_LandsatExternalTaskSensor(task, upstream_task, depend_status)

                upstream_task_id = external_task.task_id
                downstream_task_id = task_id
                DagTaskDependence.add_airflow_task_dep(dag_id, downstream_task_id, upstream_task_id)

                upstream_task_id = upstream_task.task_id
                downstream_task_id = external_task.task_id
                DagTaskDependence.add_airflow_task_dep(dag_id, downstream_task_id, upstream_task_id)
            else:
                # 添加外部依赖任务
                external_task = TaskDefine.add_LandsatExternalTaskSensor(task, upstream_task, depend_status)

                upstream_task_id = external_task.task_id
                downstream_task_id = task_id
                DagTaskDependence.add_airflow_task_dep(dag_id, downstream_task_id, upstream_task_id)

    @staticmethod
    def is_external_task_id(task_id, upstream_up_id):
        """
        根据 影子任务的 命名规则，由 external_task_id 可知，影子任务由 该任务指定格式开头
        :param task_id: 该任务
        :param upstream_up_id: 影子任务id
        :return: upstream_up_id 是影子任务返回 true，否则返回 false
        """
        return upstream_up_id.startswith("{}_wait_".format(task_id))