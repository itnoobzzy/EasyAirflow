# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import print_function, unicode_literals

import os
import time

from requests.auth import HTTPBasicAuth

from airflow.hooks.base import BaseHook
from airflow.utils.operator_helpers import AIRFLOW_VAR_NAME_FORMAT_MAPPING

from plugins.hooks.LivyBatches import polling_intervals
from plugins.hooks.YggClient import YggState

HIVE_QUEUE_PRIORITIES = ['VERY_HIGH', 'HIGH', 'NORMAL', 'LOW', 'VERY_LOW']


def get_context_from_env_var():
    """
    Extract context from env variable, e.g. dag_id, task_id and execution_date,
    so that they can be used inside BashOperator and PythonOperator.

    :return: The context of interest.
    """
    return {format_map['default']: os.environ.get(format_map['env_var_format'], '')
            for format_map in AIRFLOW_VAR_NAME_FORMAT_MAPPING.values()}


class LandsatYggInvokeHook(BaseHook):
    """
    Wrapper around the pyhive library

    Notes:
    * the default authMechanism is PLAIN, to override it you
    can specify it in the ``extra`` of your connection in the UI
    * the default for run_set_variable_statements is true, if you
    are using impala you may need to set it to false in the
    ``extra`` of your connection in the UI
    """

    def __init__(self, ygg_conn_id='ygg_default', context=None):
        super(LandsatYggInvokeHook, self).__init__()
        self.ygg_conn_id = ygg_conn_id
        self.context = context
        self.task_id = None

    def get_conn(self, schema=None):
        """
        Returns a ygg connection object.
        """
        conn = self.get_connection(self.ygg_conn_id)

        headers = None
        if conn.extra:
            try:
                headers = conn.extra_dejson
            except TypeError:
                self.log.warning('Connection to %s has invalid extra field.', conn.host)

        host = conn.host
        port = conn.port
        username = conn.login
        password = conn.password

        ygg_url = 'http://{host}:{port}'.format(host=host, port=port)

        auth = HTTPBasicAuth(username, password)

        from hooks.YggClient import YggServer
        return YggServer(
            url=ygg_url,
            auth=auth,
            headers=headers,
        )

    @staticmethod
    def _strip_sql(sql):
        return sql.strip().rstrip(';')

    def invoke_run(self, task_id, app_type, data):
        """
        Execute the statement against Hive
        """

        # 目前 Hive 不支持事务，所以，不需要 commit
        with self.get_conn() as conn:
            self.task_id = task_id
            # 调起提交任务
            conn.put_task_invoke(task_id=task_id, app_type=app_type, data=data)
            # 获取任务信息
            intervals = polling_intervals([10, 1, 2, 3, 5, 8], 10)

            status = None
            while status not in [YggState.SUCCEEDED.value, YggState.FINISHED.value, YggState.FAILED.value]:
                # 定时轮训
                time.sleep(next(intervals))
                # 获取状态
                response = conn.get_task_status(task_id, app_type=app_type)
                response_data = response['data']
                status = response_data['status']
                execId = response_data['execId']
                response_data.pop('clientLog', None)
                response_data.pop('serverLog', None)
                self.log.info('get_task_status response : {} '.format(response_data))

                # 将返回的信息存储 xcom
                if self.context is not None:
                    self.context['task_instance'].xcom_push(key='response', value=response_data)

            if status in [YggState.SUCCEEDED.value, YggState.FINISHED.value]:
                app_state = True
            else:
                app_state = False
            # 获取日志, 不支持提交日志的类型：python
            if app_type in ['python']:
                app_submit_log = "该任务类型无提交日志"
            else:
                app_submit_log = conn.get_task_submit_log(task_id, app_type=app_type)
            app_execution_log = conn.get_task_execution_log(task_id, app_type=app_type)

        return app_state, app_submit_log, app_execution_log

    def create_run(self, app_type, data):
        """
        Execute the statement against Hive
        """

        # 目前 Hive 不支持事务，所以，不需要 commit
        with self.get_conn() as conn:

            # 提交任务
            task_id = conn.create_task_not_run(data=data, app_type=app_type)
            self.task_id = task_id
            # 调起任务
            conn.put_task_invoke(task_id=task_id, app_type=app_type, data={})
            # 获取任务信息
            intervals = polling_intervals([10, 1, 2, 3, 5, 8], 10)

            status = None
            while status not in [YggState.SUCCEEDED.value, YggState.FINISHED.value, YggState.FAILED.value]:
                # 定时轮训
                time.sleep(next(intervals))
                # 获取状态
                response = conn.get_task_status(task_id, app_type=app_type)
                response_data = response['data']
                status = response_data['status']
                execId = response_data['execId']
                response_data.pop('clientLog', None)
                response_data.pop('serverLog', None)
                self.log.info('get_task_status response : {} '.format(response_data))

                # 将返回的信息存储 xcom
                if self.context is not None:
                    self.context['task_instance'].xcom_push(key='response', value=response_data)

            if status in [YggState.SUCCEEDED.value, YggState.FINISHED.value]:
                app_state = True
            else:
                app_state = False
            # 获取日志, 不支持提交日志的类型：python
            if app_type in ['python']:
                app_submit_log = "该任务类型无提交日志"
            else:
                app_submit_log = conn.get_task_submit_log(task_id, app_type=app_type)
            app_execution_log = conn.get_task_execution_log(task_id, app_type=app_type)

        return app_state, app_submit_log, app_execution_log

    def kill(self, app_type):
        """
        杀死正在运行的任务
        :return:
        """

        if self.task_id is not None:
            with self.get_conn() as conn:
                if app_type in ['python']:
                    pass
                else:
                    conn.delete_stop_task(task_id=self.task_id, app_type=app_type)
