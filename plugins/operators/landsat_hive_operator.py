# -*- coding: utf-8 -*-
#

from airflow.models import BaseOperator

# 在pycharm 中将plugins 目录设置为 source root 就不会爆红了
# 因为在 airflow.cfg 配置了plugins 目录
from hooks.landsat_hive_hook import LandsatHiveServer2Hook
from operators.utils.var_parse import VarParse


class LandsatHiveOperator(BaseOperator):
    """
    :param hql: HQL query to execute against Hive server. (templated)
    :type hql: hql
    :param hiveserver2_conn_id: destination hive connection
    :type hiveserver2_conn_id: str
        hiveserver2_conn_configuration={
            "configuration": {
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
        }
    """

    def __init__(
            self,
            *,
            hql=None,
            hiveserver2_conn_id='hiveserver2_default',
            hiveserver2_conn_configuration=None,
            parameters=None,
            **kwargs):
        super(LandsatHiveOperator, self).__init__(**kwargs)
        self.hql = hql
        self.hiveserver2_conn_id = hiveserver2_conn_id
        self.hiveserver2_conn_configuration = hiveserver2_conn_configuration
        if self.hiveserver2_conn_configuration is None:
            self.hiveserver2_conn_configuration = {}
        self.parameters = parameters
        self.hook = None

    def execute(self, context):
        self.log.info('Executing: %s', self.hql)
        self.hql = VarParse.operator_re_replace_datetime_var(self.hql, context)
        self.log.info('Executing really: %s', self.hql)
        #  去掉末尾的 ; ,并对 sql 按照 ;\n 进行拆分
        self.hql = VarParse.split_sql(self.hql)
        self.log.info('Split SQL really: %s', self.hql)
        try:
            self.hook = LandsatHiveServer2Hook(hiveserver2_conn_id=self.hiveserver2_conn_id,
                                               hiveserver2_conn_configuration=self.hiveserver2_conn_configuration)
            self.hook.run(self.hql, parameters=self.parameters)
        except Exception as e:
            self.log.error('Executing failed!')
            raise
        self.log.info("Done.")

    def on_kill(self):
        if self.hook:
            self.hook.kill()
