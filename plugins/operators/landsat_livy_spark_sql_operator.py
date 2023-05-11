# -*- coding: utf-8 -*-
#

from tempfile import NamedTemporaryFile

from airflow.hooks.hive_hooks import HiveServer2Hook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.operator_helpers import context_to_airflow_vars

from operators.utils.var_parse import VarParse


class LandsatLivySparkSQLOperator(BaseOperator):
    """
    :param sql: sql query to execute against Hive server. (templated)
    :type sql: sql
    :param livy_conn_id: destination hive connection
    :type livy_conn_id: str
    """


    @apply_defaults
    def __init__(
            self,
            sql,
            livy_conn_id='livy_default',
            parameters=None,
            *args, **kwargs):
        super(LandsatLivySparkSQLOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.livy_conn_id = livy_conn_id
        self.parameters=parameters

    def execute(self, context):

        self.log.info('Executing: %s', self.sql)

        self.sql = VarParse.operator_re_replace_datetime_var(self.sql, context)

        self.log.info('Executing really: %s', self.sql)

        #  去掉末尾的 ; ,并对 sql 按照 ;\n 进行拆分
        self.sql = VarParse.split_sql(self.sql)
        self.log.info('Split SQL really: %s', self.sql)

        try:
            from hooks.landsat_livy_spark_sql_hook import LandsatLivySparkSQLHook
            hook = LandsatLivySparkSQLHook(livy_conn_id=self.livy_conn_id)
            # hook.run(self.sql,parameters=self.parameters)
            hook.run(self.sql)

        except Exception as e:
            self.log.error('Executing failed: {} ', self.sql)
            raise

        self.log.info("Done.")
