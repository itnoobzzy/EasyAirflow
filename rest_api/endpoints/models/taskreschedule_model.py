#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:wanglong

from sqlalchemy import (Column,
                        Integer, String, DateTime)
from airflow.models.taskinstance import TaskReschedule


class EasyAirflowTaskReschedule(TaskReschedule):

    def clear_task_reschedule(self, dag_id, task_id, execution_date):
        """
        清理重试记录
        :param dag_id:
        :param task_id:
        :param execution_date:
        :return:
        """
        self.query.filter_by(dag_id=dag_id, task_id=task_id, execution_date=execution_date).delete()
        self.commit()