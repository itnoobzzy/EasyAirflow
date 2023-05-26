# -*- coding: utf-8 -*-
#

from typing import Any, Dict, List

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from plugins.operators.utils.encryption import str_decryption, str_encryption
from plugins.operators.utils.var_parse import VarParse
from datetime import datetime

class LandsatLivySparkBatchOperator(BaseOperator):
    """
    :param sql: sql query to execute against Hive server. (templated)
    :type sql: sql
    :param livy_conn_id: destination hive connection
    :type livy_conn_id: str
    """

    """
    约定：对于 jar 的 参数 app_args 所有的参数传入都用  base64 编码
    """
    @apply_defaults
    def __init__(
            self,
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
            queue_name: str = None,
            name: str = None,
            spark_conf: Dict[str, Any] = None,

            file: str = None,
            className: str = None,
            app_args: List[str] = None,

            livy_conn_id: str='livy_default',
            *args, **kwargs):

        super(LandsatLivySparkBatchOperator, self).__init__(*args, **kwargs)
        self.proxy_user = proxy_user
        self.jars = jars
        self.py_files = py_files
        self.files = files
        self.driver_memory = driver_memory
        if self.driver_memory is None:
            self.driver_memory = '4g'
        self.driver_cores = driver_cores
        self.executor_memory = executor_memory
        if self.executor_memory is None:
            self.executor_memory = '4g'
        self.executor_cores = executor_cores
        if self.executor_cores is None:
            self.executor_cores = 2
        self.num_executors = num_executors
        if self.num_executors is None:
            self.num_executors = 3
        self.archives = archives
        self.queue_name = queue_name
        self.name = name
        self.spark_conf = spark_conf

        self.file = file
        self.className = className
        self.app_args = []

        # 对所有的 jar 包参数进行解密
        for item in app_args:
            de_item = str_decryption(item)
            self.app_args.append(de_item)

        self.livy_conn_id = livy_conn_id

        self.hook = None

    def re_replace_datetime_var(self, context):
        """
        替换 sql 中和 执行计划相关的变量
        :param execution_date_timestamp:
        :return:
        """
        complete_app_args = []
        for app_arg in self.app_args:

            complete_app_arg = VarParse.operator_re_replace_datetime_var(app_arg, context)

            # 如果参数前后发生了变化，说明是有替换发生了，应该是执行的SQL
            # if complete_app_arg != app_arg:
            #     self.log.info('Executing really SQL : %s', complete_app_arg)

            """
            约定：对于 jar 的 参数 app_args 所有的参数传入都用  base64 编码
            """
            complete_app_args.append(str_encryption(complete_app_arg))

        return complete_app_args

    def execute(self, context):

        self.log.info('Executing: %s', self.name)

        # self.log.info('Before Executing: %s', self.app_args)

        self.app_args = self.re_replace_datetime_var(context)

        self.log.info('After Executing: %s', self.app_args)

        execution_date = context['execution_date']
        unique_name =  "{name}>{timestamp}".format(
            name=self.name,
            timestamp = datetime.now().timestamp()
        )
        self.name = unique_name

        try:

            from hooks.landsat_livy_spark_batch_hook import LandsatLivySparkBatchHook
            self.hook = LandsatLivySparkBatchHook(livy_conn_id=self.livy_conn_id,context=context)
            app_state, app_log = self.hook.run(
                proxy_user = self.proxy_user
                ,jars = self.jars
                ,py_files = self.py_files
                ,files = self.files
                ,driver_memory = self.driver_memory
                ,driver_cores = self.driver_cores
                ,executor_memory = self.executor_memory
                ,executor_cores = self.executor_cores
                ,num_executors = self.num_executors
                ,archives = self.archives
                ,queue = self.queue_name
                ,name = self.name
                ,spark_conf = self.spark_conf

                ,file = self.file
                ,className = self.className
                ,args = self.app_args
            )

            self.log.info('======> Livy log start:')

            app_log = '\n'.join(app_log)
            self.log.info('{}'.format(app_log))
            
            self.log.info('======> Livy log end.')

            if app_state is False:
                raise

        except Exception as e:
            self.log.error('Executing failed: {} ', self.name)
            raise

        self.log.info("Done.")


    def on_kill(self):
        if self.hook:
            self.hook.kill()