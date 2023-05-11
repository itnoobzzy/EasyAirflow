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
from typing import Any, Dict, List

from requests.auth import HTTPBasicAuth

from airflow.hooks.base import BaseHook
from airflow.utils.operator_helpers import AIRFLOW_VAR_NAME_FORMAT_MAPPING

from hooks.LivyBatches import polling_intervals, BatchesState

HIVE_QUEUE_PRIORITIES = ['VERY_HIGH', 'HIGH', 'NORMAL', 'LOW', 'VERY_LOW']


def get_context_from_env_var():
    """
    Extract context from env variable, e.g. dag_id, task_id and execution_date,
    so that they can be used inside BashOperator and PythonOperator.

    :return: The context of interest.
    """
    return {format_map['default']: os.environ.get(format_map['env_var_format'], '')
            for format_map in AIRFLOW_VAR_NAME_FORMAT_MAPPING.values()}



class LandsatLivySparkBatchHook(BaseHook):
    """
    Wrapper around the pyhive library

    Notes:
    * the default authMechanism is PLAIN, to override it you
    can specify it in the ``extra`` of your connection in the UI
    * the default for run_set_variable_statements is true, if you
    are using impala you may need to set it to false in the
    ``extra`` of your connection in the UI
    """
    def __init__(self, livy_conn_id='livy_default', context=None):
        super(LandsatLivySparkBatchHook, self).__init__()
        self.livy_conn_id = livy_conn_id
        self.context = context

        self.batchId = None

    def get_conn(self, schema=None):
        """
        Returns a Livy connection object.
        """
        db = self.get_connection(self.livy_conn_id)

        host=db.host
        port=db.port
        username=db.login
        password=db.password

        livy_url = 'http://{host}:{port}'.format(host=host,port=port)

        auth = HTTPBasicAuth(username, password)

        from hooks.LivyBatches import Batches
        return Batches(
            url=livy_url,
            auth=auth,
        )

    @staticmethod
    def _strip_sql(sql):
        return sql.strip().rstrip(';')

    def run(self,
            proxy_user: str = None,
            jars: List[str] = None,
            py_files: List[str] = None,
            files: List[str] = None,
            driver_memory: str = None,
            driver_cores: int = None,
            executor_memory: str = None,
            executor_cores: int = None,
            num_executors: int = None,
            archives: List[str] = None,
            queue: str = None,
            name: str = None,
            spark_conf: Dict[str, Any] = None,

            file: str = None,
            className: str = None,
            args: List[str] = None,
            ):
        """
        Execute the statement against Hive
        """


        # 目前 Hive 不支持事务，所以，不需要 commit
        with self.get_conn() as conn:
            # app_state, app_log = conn.run(
            #                 proxy_user=proxy_user,
            #                 jars=jars,
            #                 py_files=py_files,
            #                 files=files,
            #                 driver_memory=driver_memory,
            #                 driver_cores=driver_cores,
            #                 executor_memory=executor_memory,
            #                 executor_cores=executor_cores,
            #                 num_executors=num_executors,
            #                 archives=archives,
            #                 queue=queue,
            #                 name=name,
            #                 spark_conf=spark_conf,
            #
            #                 file=file,
            #                 className=className,
            #                 args=args
            #             )

            # 提交任务
            batchId, state = conn.post_batches(
                            proxy_user=proxy_user,
                            jars=jars,
                            py_files=py_files,
                            files=files,
                            driver_memory=driver_memory,
                            driver_cores=driver_cores,
                            executor_memory=executor_memory,
                            executor_cores=executor_cores,
                            num_executors=num_executors,
                            archives=archives,
                            queue=queue,
                            name=name,
                            spark_conf=spark_conf,

                            file=file,
                            className=className,
                            args=args
                        )
            self.batchId = batchId
            # 获取任务信息
            intervals = polling_intervals([1, 2, 3, 5, 8], 10)

            while state not in [BatchesState.SUCCESS.value, BatchesState.DEAD.value]:
                time.sleep(next(intervals))
                response = conn.get_batches(batchId)
                appId = response['appId']
                state = response['state']
                response.pop('log')
                self.log.info('get_batches response : {} '.format(response))

                if self.context is not None:
                    self.context['task_instance'].xcom_push(key='response', value=response)

            if state in [BatchesState.SUCCESS.value]:
                app_state = True
            else:
                app_state = False

            app_log = conn.get_batches_log(batchId)
            # 获取任务状态
            pass

        return app_state, app_log


    def kill(self):
        """
        杀死正在运行的任务
        :return:
        """
        if self.batchId is not None:
            with self.get_conn() as conn:
                conn.delete_stop_task(batchId=self.batchId)
        pass