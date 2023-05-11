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
import sys
from past.builtins import basestring

from airflow.hooks.base import BaseHook
from airflow.utils.operator_helpers import AIRFLOW_VAR_NAME_FORMAT_MAPPING
from sqlalchemy import create_engine, text

HIVE_QUEUE_PRIORITIES = ['VERY_HIGH', 'HIGH', 'NORMAL', 'LOW', 'VERY_LOW']


def get_context_from_env_var():
    """
    Extract context from env variable, e.g. dag_id, task_id and execution_date,
    so that they can be used inside BashOperator and PythonOperator.

    :return: The context of interest.
    """
    return {format_map['default']: os.environ.get(format_map['env_var_format'], '')
            for format_map in AIRFLOW_VAR_NAME_FORMAT_MAPPING.values()}


class LandsatPrestoHook(BaseHook):
    """
    Wrapper around the pyhive library

    Notes:
    * the default authMechanism is PLAIN, to override it you
    can specify it in the ``extra`` of your connection in the UI
    * the default for run_set_variable_statements is true, if you
    are using impala you may need to set it to false in the
    ``extra`` of your connection in the UI
    """

    def __init__(self, presto_conn_id='presto_conn_id'):
        super(LandsatPrestoHook, self).__init__()
        self.presto_conn_id = presto_conn_id

    def get_conn(self, schema=None):
        """
        Returns a Presto connection object.
        """
        db = self.get_connection(self.presto_conn_id)

        host = db.host
        port = db.port
        username = db.login
        password = db.password
        catalog = db.extra_dejson.get('catalog', 'hive')
        schema = schema or db.schema or 'default'
        protocol = db.extra_dejson.get('protocol', 'https')
        session_props = self.get_configuration()

        presto_url = 'presto://{username}:{password}@{host}:{port}/{catalog}?schema={schema}'.format(
            host=host
            , port=port
            , username=username
            , password=password
            , catalog=catalog
            , schema=schema
        )

        connect_args = {
            "protocol": protocol
            , 'session_props': session_props
        }

        return create_engine(
            presto_url,
            connect_args=connect_args
        ).connect()

    def get_configuration(self):
        """
        configuration = {
            'query_max_run_time': '1234m'
        }
        :return:
        """

        db = self.get_connection(self.presto_conn_id)
        configuration = db.extra_dejson.get('session_props', {})
        self.log.info("Run with parameters {}".format(configuration))

        return configuration

    @staticmethod
    def _strip_sql(sql):
        return sql.strip().rstrip(';')

    @staticmethod
    def _split_sql(sql):
        return sql.split(';')

    def run(self, sql, parameters=None):
        """
        Execute the statement against Presto
        """

        if isinstance(sql, basestring):
            sql = [sql]

        # 目前 Presto 不支持事务，所以，不需要 commit，至少 pyhive 没有实现事务
        with contextlib.closing(self.get_conn()) as cur:
            # with contextlib.closing(conn.cursor()) as cur:
            for s in sql:
                # if sys.version_info[0] < 3:
                #     s = s.encode('utf-8')
                s = text(s)
                if parameters is not None:
                    self.log.info("{} with parameters {}".format(s, parameters))
                    cur.execute(s, parameters).scalar()
                else:
                    self.log.info(s)
                    cur.execute(s).scalar()
                # self.log.info("Return Value:\n {}".format(cur.scalar()))
