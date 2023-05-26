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

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from plugins.hooks.landsat_presto_hook import LandsatPrestoHook
from plugins.operators.utils.var_parse import VarParse


class LandsatPrestoOperator(BaseOperator):
    """
    Moves data from Presto to MySQL, note that for now the data is loaded
    into memory before being pushed to MySQL, so this operator should
    be used for smallish amount of data.

    :param sql: SQL query to execute against Presto. (templated)
    :type sql: str
    :param mysql_table: target MySQL table, use dot notation to target a
        specific database. (templated)
    :type mysql_table: str
    :param mysql_conn_id: source mysql connection
    :type mysql_conn_id: str
    :param presto_conn_id: source presto connection
    :type presto_conn_id: str
    :param mysql_preoperator: sql statement to run against mysql prior to
        import, typically use to truncate of delete in place
        of the data coming in, allowing the task to be idempotent (running
        the task twice won't double load data). (templated)
    :type mysql_preoperator: str
    """

    @apply_defaults
    def __init__(
            self,
            sql,
            presto_conn_id='presto_default',
            parameters=None,
            *args, **kwargs):
        super(LandsatPrestoOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.presto_conn_id = presto_conn_id
        self.parameters = parameters


    def execute(self, context):
        # presto = PrestoHook(presto_conn_id=self.presto_conn_id)
        # self.log.info("Extracting data from Presto: %s", self.sql)
        # results = presto.get_records(self.sql)

        self.log.info('Executing: %s', self.sql)

        self.sql = VarParse.operator_re_replace_datetime_var(self.sql, context)

        self.log.info('Executing really: %s', self.sql)


        #  去掉末尾的 ; ,并对 sql 按照 ;\n 进行拆分
        self.sql = VarParse.split_sql(self.sql)
        self.log.info('Split SQL really: %s', self.sql)

        try:

            hook = LandsatPrestoHook(presto_conn_id=self.presto_conn_id)
            hook.run(self.sql,parameters=self.parameters)

        except Exception as e:
            # self.log.error('Executing failed: {} ', self.sql)
            self.log.error('Executing failed!')
            raise

        self.log.info("Done.")
