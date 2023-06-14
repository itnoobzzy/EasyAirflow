#!/usr/bin/env python
# -*- coding:utf-8 -*-
import logging

from airflow.exceptions import DagNotFound
from airflow.models import DagModel, DagBag
from airflow.utils.state import State

from rest_api.endpoints.models.dagrun_model import EasyAirflowDagRun
from rest_api.endpoints.models.task_instance_model import EasyAirflowTaskInstance

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
    def get_dag(dag_id):
        dag_model = DagModel.get_current(dag_id)
        if dag_model is None:
            raise DagNotFound("Dag id {} not found in DagModel".format(dag_id))
        dag_bag = DagBag()
        dag = DagBag().get_dag(dag_id)
        if dag_id not in dag_bag.dags:
            raise DagNotFound("Dag id {} not found".format(dag_id))
        return dag

    @classmethod
    def back_fill(cls, dag_id, task_id, start_date, end_date):
        """
        补数据：直接生成对应计划执行时间的任务实例
        获取对应的 executor，将序列化的 dag 直接提交到 executor 中执行，不需要等待
        :param dag_id: dag id
        :param task_id: task_id
        :param execution_date: 计划执行时间
        :return:
        """
        task_regex = f"^{task_id}$"
        dag = cls.get_dag(dag_id)
        task = dag.get_task(task_id)
        dag = dag.partial_subset(
            task_ids_or_regex=task_regex,
            include_upstream=False
        )
        dag.run(
            start_date=start_date,
            donot_pickle=True,
            end_date=end_date,
            ignore_first_depends_on_past=True,
            ignore_task_deps=True,
            pool=task.pool
        )
        # from airflow.executors.celery_executor import CeleryExecutor
        # from airflow.jobs.job import Job
        # from airflow.jobs.job import run_job
        # from airflow.jobs.backfill_job_runner import BackfillJobRunner
        #
        # executor = CeleryExecutor
        # job = Job(executor=executor)
        # job_runner = BackfillJobRunner(
        #     job=job,
        #     dag=dag,
        #     start_date=start_date,
        #     end_date=end_date,
        #     mark_success=False,
        #     donot_pickle=True,
        #     ignore_task_deps=True,
        #     ignore_first_depends_on_past=True,
        #     pool=task.pool,
        #     delay_on_limit_secs=1.0,
        #     verbose=False,
        #     conf=None,
        #     rerun_failed_tasks=False,
        #     run_backwards=False,
        #     run_at_least_once=False,
        #     continue_on_failures=False,
        #     disable_retry=False,
        # )
        # run_job(job=job, execute_callable=job_runner._execute)


