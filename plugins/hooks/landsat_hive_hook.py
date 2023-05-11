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

import contextlib
import os
from typing import Iterable, Mapping, Callable

from TCLIService.ttypes import TOperationState
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
from past.builtins import basestring

from airflow.utils.operator_helpers import AIRFLOW_VAR_NAME_FORMAT_MAPPING

HIVE_QUEUE_PRIORITIES = ['VERY_HIGH', 'HIGH', 'NORMAL', 'LOW', 'VERY_LOW']


def get_context_from_env_var():
    """
    Extract context from env variable, e.g. dag_id, task_id and execution_date,
    so that they can be used inside BashOperator and PythonOperator.

    :return: The context of interest.
    """
    return {format_map['default']: os.environ.get(format_map['env_var_format'], '')
            for format_map in AIRFLOW_VAR_NAME_FORMAT_MAPPING.values()}


class LandsatHiveServer2Hook(HiveServer2Hook):
    """
    Wrapper around the pyhive library

    Notes:
    * the default authMechanism is PLAIN, to override it you
    can specify it in the ``extra`` of your connection in the UI
    * the default for run_set_variable_statements is true, if you
    are using impala you may need to set it to false in the
    ``extra`` of your connection in the UI
    """

    def __init__(self, hiveserver2_conn_id='hiveserver2_default', hiveserver2_conn_configuration=None):
        super(LandsatHiveServer2Hook, self).__init__()
        self.hiveserver2_conn_id = hiveserver2_conn_id
        self.hiveserver2_conn_configuration = hiveserver2_conn_configuration
        self.killed = False
        self.cursor = None

    def get_conn(self, schema=None):
        """
        Returns a Hive connection object.
        """
        db = self.get_connection(self.hiveserver2_conn_id)

        host = db.host
        port = db.port
        username = db.login
        password = db.password
        database = schema or db.schema or 'default'

        auth = db.extra_dejson.get('auth', 'LDAP')
        configuration = self.get_configuration()
        configuration['auth'] = auth

        connect_args = {**configuration, **self.hiveserver2_conn_configuration}

        connect_args = connect_args.get('configuration', None)
        self.log.info("Run with connect_args {}".format(connect_args))
        from pyhive.hive import connect
        return connect(
            host=host,
            port=port,
            username=username,
            password=password,
            auth=auth,
            database=database,
            configuration=connect_args
        )

    def get_configuration(self):
        """
        configuration = {
            "hive.server2.session.check.interval": "7200000",
            "hive.server2.idle.operation.timeout": "7200000",
            "hive.server2.idle.session.timeout": "7200000",
            "mapreduce.job.queuename": "root.batch.etl",
            # "mapreduce.map.memory.mb": "8000",
            # "mapreduce.map.java.opts": "-Xmx7200m",
            # "mapreduce.reduce.memory.mb": "8000",
            # "mapreduce.reduce.java.opts": "-Xmx7200m",
            # "hive.exec.parallel": "true",
            # "hive.exec.parallel.thread.number": "3",
            # "mapred.max.split.size": "256000000",
            # "mapred.min.split.size.per.node": "256000000",
            # "mapred.min.split.size.per.rack": "256000000",
            # "hive.exec.reducers.bytes.per.reducer": "256000000",
            # "hive.input.format": "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat"
            # "mapreduce.job.max.split.locations": "50"
        }
        :return:
        """

        db = self.get_connection(self.hiveserver2_conn_id)
        configuration = db.extra_dejson
        self.log.info("Run with parameters {}".format(configuration))
        return configuration

    @staticmethod
    def _strip_sql(sql):
        return sql.strip().rstrip(';')

    @staticmethod
    def _split_sql(sql):
        return sql.split(';')

    def run(
        self,
        sql: str | Iterable[str],
        autocommit: bool = False,
        parameters: Iterable | Mapping | None = None,
        handler: Callable = None,
        split_statements: bool = False,
        return_last: bool = True,
    ) -> None:
        """
        Execute the statement against Hive
        """
        STATE_MAP = {
            TOperationState.INITIALIZED_STATE: "The operation has been initialized",
            TOperationState.RUNNING_STATE: "The operation is running. In this state the result set is not available",
            TOperationState.FINISHED_STATE: "The operation has completed. When an operation is in this state its result set may be fetched",
            TOperationState.CANCELED_STATE: "The operation was canceled by a client",
            TOperationState.CLOSED_STATE: "The operation was closed by a client",
            TOperationState.ERROR_STATE: "The operation failed due to an error",
            TOperationState.UKNOWN_STATE: "The operation is in an unrecognized state",
            TOperationState.PENDING_STATE: "The operation is in an pending state",
            TOperationState.TIMEDOUT_STATE: "The operation is in an timedout state",
        }
        if isinstance(sql, basestring):
            sql = [sql]
        with contextlib.closing(self.get_conn()) as conn:
            with contextlib.closing(conn.cursor()) as cur:
                self.cursor = cur
                for s in sql:
                    # https://github.com/dropbox/PyHive/tree/v0.6.2#db-api-asynchronous
                    self.log.info("<-------------------------  run start  ------------------------->")
                    self.cursor.execute(s, async_=True)
                    status = self.cursor.poll().operationState
                    while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
                        logs = self.cursor.fetch_logs()
                        for message in logs:
                            self.log.info(message)
                        status = self.cursor.poll().operationState
                    try:
                        end_time = 0
                        if status == 5:
                            self.log.info('=======================执行hive报错===================')
                            while end_time < 20:
                                end_time += 1
                                logs = self.cursor.fetch_logs()
                                for message in logs:
                                    self.log.info(message)
                    except Exception as e:
                        self.log.info(f"执行hive报错, 获取日志异常: {e}")
                    self.log.info("run end status %s", status)
                    self.log.info("run end status %s", STATE_MAP[status])

                    if status in (
                            TOperationState.CANCELED_STATE,
                            TOperationState.CLOSED_STATE,
                            TOperationState.ERROR_STATE,
                            TOperationState.UKNOWN_STATE,
                            TOperationState.TIMEDOUT_STATE
                    ):
                        raise Exception("run end status {}".format(STATE_MAP[status]))
                    elif status in [TOperationState.PENDING_STATE]:
                        raise Exception("run end status {}".format(STATE_MAP[status]))
                    else:
                        pass

                    self.log.info("<-------------------------  run end  ------------------------->")

    def kill(self):
        """
        杀死正在运行的任务
        :return:
        """
        if self.cursor is not None:
            self.log.info("cancel hive query!!!")
            self.cursor.cancel()
