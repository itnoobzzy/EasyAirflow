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
import sys
from past.builtins import basestring

from airflow.hooks.base import BaseHook
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



class LandsatLivySparkSQLHook(BaseHook):
    """
    Wrapper around the pyhive library

    Notes:
    * the default authMechanism is PLAIN, to override it you
    can specify it in the ``extra`` of your connection in the UI
    * the default for run_set_variable_statements is true, if you
    are using impala you may need to set it to false in the
    ``extra`` of your connection in the UI
    """
    def __init__(self, livy_conn_id='livy_default'):
        super(LandsatLivySparkSQLHook, self).__init__()
        self.livy_conn_id = livy_conn_id

    def get_conn(self, schema=None):
        """
        Returns a Livy connection object.
        """
        db = self.get_connection(self.livy_conn_id)

        from livy import LivySession, SessionKind
        from requests.auth import HTTPBasicAuth

        host=db.host
        port=db.port
        username=db.login
        password=db.password

        livy_url = 'http://{host}:{port}'.format(host=host,port=port)

        auth = HTTPBasicAuth(username, password)

        return LivySession(
            url=livy_url,
            auth=auth,
            kind=SessionKind.SQL
        )

    @staticmethod
    def _strip_sql(sql):
        return sql.strip().rstrip(';')

    def run(self, sql):
        """
        Execute the statement against Hive
        """
        if isinstance(sql, basestring):
            sql = [sql]

        # 目前 Hive 不支持事务，所以，不需要 commit
        # with contextlib.closing(self.get_conn()) as conn:
        with self.get_conn() as conn:
            # with contextlib.closing(conn.cursor()) as cur:
                for s in sql:
                    if sys.version_info[0] < 3:
                        s = s.encode('utf-8')
                    self.log.info(s)
                    conn.run(s)



