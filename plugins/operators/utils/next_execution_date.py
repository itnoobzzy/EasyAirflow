#!/usr/bin/env python
# -*- coding:utf-8 -*-


class NextExecutionDate(object):

    @staticmethod
    def get_task_instance_next_execution(context):
        task_instance = context['task_instance']
        dag_id = task_instance.dag_id
        task_id = task_instance.task_id
        execution_date = task_instance.execution_date

        from airflow.models.dagrun import TaskInstanceNext # noqa

        task_instance_next = TaskInstanceNext.get_task_instance_next(dag_id,task_id,execution_date)
        next_execution_date = None
        if task_instance_next is not None:
            next_execution_date = task_instance_next.next_execution_date

        return next_execution_date

