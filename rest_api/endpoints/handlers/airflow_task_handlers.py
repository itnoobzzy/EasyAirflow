#!/usr/bin/env python
# -*- coding:utf-8 -*-

import json
import logging

import pendulum
from airflow.exceptions import DagNotFound
from airflow.models import DagModel, DagBag
from airflow.utils.state import State
from croniter import croniter
from airflow.executors.celery_executor import CeleryExecutor
from airflow.executors.kubernetes_executor import KubernetesExecutor
from airflow.executors.executor_loader import ExecutorLoader

from endpoints.models.dagrun_model import EasyAirflowDagRun
from endpoints.models.log_model import Log
from endpoints.models.task_instance_model import EasyAirflowTaskInstance
from endpoints.models.task_model import TaskDefine
from config import SERVE_LOG_PROT
from utils.airflow_database import airflow_provide_session
from utils.times import datetime_convert_pendulum_by_timezone, datetime_timestamp_str

logger = logging.getLogger(__name__)


class TaskHandlers(object):

    @staticmethod
    def clear_task_instances(tis):
        """
        重跑任务实例
        :param tis: a list of task instances
        :param activate_dag_runs: flag to check for active dag run
        """
        dag_ids = []
        execution_dates = []
        for ti in tis:
            dag_ids.append(ti.dag_id)
            execution_dates.append(ti.execution_date)
            EasyAirflowTaskInstance.shutdown_ti(ti, State.NONE)

        if dag_ids and execution_dates:
            EasyAirflowDagRun.trigger_dags(dag_ids, execution_dates)

    @staticmethod
    def stop_task_instances(tis):
        """
        停止任务实例
        :param tis: a list of task instances
        """
        for ti in tis:
            EasyAirflowTaskInstance.shutdown_ti(ti, State.SHUTDOWN)

    @staticmethod
    def rerun_task_instances(tis):
        """
        周期和补数据类型都可以运行
        :param tis:
        :return:
        """
        for ti in tis:
            # dag_id = ti.dag_id
            dag_id = getattr(ti, 'new_dag_id')
            task_id = ti.task_id
            execution_date = ti.execution_date
            # 运行指定任务实例
            TaskHandlers.direct_run_task(dag_id, task_id, execution_date)

    @staticmethod
    @airflow_provide_session
    def get_task_log_url(dag_id, task_id, execution_date, try_number=None, session=None):
        # todo 需要从配置获取
        port = SERVE_LOG_PROT

        ti = EasyAirflowTaskInstance.get_task_instance(dag_id, task_id, execution_date)

        dag_id = ti.dag_id
        task_id = ti.task_id

        if try_number is None:
            try_number = ti._try_number

        hostname = TaskHandlers.query_task_hostname(ti, try_number)

        if hostname is None:
            hostname = ti.hostname

        execution_date = ti.execution_date

        execution_date_str = datetime_convert_pendulum_by_timezone(execution_date)

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

        return url,ti._try_number

    @staticmethod
    @airflow_provide_session
    def query_task_hostname(ti, try_number, session=None):
        """
        Get task log hostname by log table and try_number
         :param ti: task instance record
        :param try_number: current try_number to read log from
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        :return: log message hostname
        """
        """
        select * from log 
        where event = 'cli_run' 
        and  task_id = 'daily_delete_afi_risk_extension' 
        and  execution_date = '2020-05-25 21:55:00.000000' 
        group by extra order by id
        """

        """
        select extra from log 
        where event = 'cli_run' 
        and  task_id = 'daily_delete_afi_risk_extension' 
        and  execution_date = '2020-05-25 21:55:00.000000' 
        group by extra 
        order by extra desc 
        """



        res_extra_id_list = session.query(Log.extra,Log.id).filter(
            Log.dag_id == ti.dag_id,
            Log.task_id == ti.task_id,
            Log.execution_date == ti.execution_date,
            Log.owner != 'anonymous',
            Log.event == 'cli_run',
            # Log.extra.like('%--raw%')
        ).all()

        extra_id_dict = {}
        for res_extra_id in res_extra_id_list:
            # res_extra = res_extra_id[0]
            # res_id  = res_extra_id[1]

            res_extra = res_extra_id.extra
            res_id  = res_extra_id.id


            extra_id_dict[res_extra] = res_id

        # 参考： https://stackoverflow.com/questions/613183/how-do-i-sort-a-dictionary-by-value
        # 按照 id 进行排序，升序
        sorted_log_id_dict = sorted(extra_id_dict.items(), key=lambda x: x[1])
        # 根据重试次数，获取排序编号
        # res_extra_id = sorted_log_id_dict[try_number - 1]
        all_log = len(list(extra_id_dict.keys()))
        log_index = min(try_number ,all_log)

        res_extra_id = sorted_log_id_dict[log_index - 1]
        # 获取 res_extra 信息，提取 host_name
        res_extra = res_extra_id[0]
        hostname = json.loads(res_extra)["host_name"]
        return hostname

    @staticmethod
    def get_dag(dag_id):
        dag_model = DagModel.get_current(dag_id)
        if dag_model is None:
            raise DagNotFound("Dag id {} not found in DagModel".format(dag_id))

        def read_store_serialized_dags():
            from airflow.configuration import conf
            return conf.getboolean('core', 'store_serialized_dags')

        dag_bag = DagBag(
            dag_folder=dag_model.fileloc,
            store_serialized_dags=read_store_serialized_dags()
        )
        dag = dag_bag.get_dag(dag_id)
        if dag_id not in dag_bag.dags:
            raise DagNotFound("Dag id {} not found".format(dag_id))
        return dag

    @classmethod
    def back_fill(cls, dag_id, task_id, execution_date):
        """
        补数据：直接生成对应计划执行时间的任务实例
        获取对应的 executor，将序列化的 dag 直接提交到 executor 中执行，不需要等待
        :param dag_id: dag id
        :param task_id: task_id
        :param execution_date: 计划执行时间
        :return:
        """
        dag = cls.get_dag(dag_id)
        task = dag.get_task(task_id)

        execution_date = pendulum.parse(execution_date)
        ignore_all_deps = True
        ignore_task_deps = True
        ignore_ti_state = True

        executor = ExecutorLoader.get_default_executor()
        valid_celery_config = isinstance(executor, CeleryExecutor)
        valid_kubernetes_config = isinstance(executor, KubernetesExecutor)

        if not valid_celery_config and not valid_kubernetes_config:
            raise Exception("Only works with the Celery or Kubernetes executors, sorry")

        ti = EasyAirflowTaskInstance(task=task, execution_date=execution_date)
        ti.refresh_from_db()
        pickle = dag.pickle()

        executor.start()
        executor.queue_task_instance(
            ti,
            pickle_id=pickle.id,
            ignore_all_deps=ignore_all_deps,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state)
        executor.heartbeat()
        logger.info(f"Sent {ti} to the message queue. it should start any moment now.")

    @classmethod
    def complement_task_instances(cls, task_id, execution_next_date_timestamp_list):
        """
        调用直接run的接口，运行指定的的实例
        :param task_id: 任务id
        :param execution_next_date_timestamp_list: 执行时间的时间撮格式,单位为 ms
        :return:
        """
        airflow_task = TaskDefine.get_task(task_id)
        dag_id = airflow_task.dag_id
        crontab_str = airflow_task.schedule_interval

        for execution_next_date_timestamp in execution_next_date_timestamp_list:
            # 获取当前执行时间的前一个执行周期
            iter = croniter(crontab_str, execution_next_date_timestamp / 1000)
            execution_date_timestamp = iter.get_prev() * 1000

            # 转化为0时区时间和字符串
            execution_date = datetime_timestamp_str(int(execution_date_timestamp))

            # 补数据
            cls.back_fill(dag_id, task_id, execution_date)
