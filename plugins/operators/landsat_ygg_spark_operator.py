# -*- coding: utf-8 -*-
#

from typing import Any, Dict, List

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from operators.utils.encryption import str_decryption, str_encryption
from operators.utils.var_parse import VarParse
from datetime import datetime

class LandsatYggSparkOperator(BaseOperator):
    """
    :param sql: sql query to execute against Hive server. (templated)
    :type sql: sql
    :param ygg_conn_id: destination hive connection
    :type ygg_conn_id: str
    """

    """
    约定：对于 jar 的 参数 app_args 所有的参数传入都用  base64 编码
    """
    @apply_defaults
    def __init__(
            self,
            engineParams: List[str] = None,
            taskParams: List[str] = None,

            environment: int = None,
            task_name: str = '',
            task_desc: str = '',

            user_id: int = None,
            # username: str = None,

            ygg_conn_id: str='ygg_default',
            *args, **kwargs):

        super(LandsatYggSparkOperator, self).__init__(*args, **kwargs)
        self.engineParams = engineParams
        # self.taskParams = taskParams
        self.taskParams = []

        self.environment = environment
        self.task_name = task_name
        self.task_desc = task_desc
        self.user_id = user_id




        # 对所有的 jar 包参数进行解密
        # for item in taskParams:
        #     de_item = str_decryption(item)
        #     self.taskParams.append(de_item)

        self.taskParams = taskParams

        self.ygg_conn_id = ygg_conn_id

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
            if complete_app_arg != app_arg:
                self.log.info('Executing really SQL : %s', complete_app_arg)

            """
            约定：对于 jar 的 参数 app_args 所有的参数传入都用  base64 编码
            """
            complete_app_args.append(str_encryption(complete_app_arg))

        return complete_app_args

    def execute(self, context=None):

        self.log.info('Executing: %s', self.engineParams)

        # self.log.info('Executing: %s', self.name)
        #
        # self.app_args = self.re_replace_datetime_var(context)
        #
        # # self.log.info('Executing: %s', self.app_args)
        #
        # execution_date = context['execution_date']
        # unique_name =  "{name}>{timestamp}".format(
        #     name=self.name,
        #     timestamp = datetime.now().timestamp()
        # )
        # self.name = unique_name

        try:

            from hooks.landsat_ygg_spark_hook import LandsatYggSparkHook
            self.hook = LandsatYggSparkHook(ygg_conn_id=self.ygg_conn_id,context=context)
            app_state, app_submit_log, app_execution_log = self.hook.run(
                engineParams = self.engineParams,
                taskParams = self.taskParams,
                environment = self.environment,
                task_name = self.task_name,
                task_desc = self.task_desc,
                user_id = self.user_id,
            )

            self.log.info('======> Ygg log start:')

            self.log.info('======> Ygg log submit:')
            # app_log = '\n'.join(app_submit_log)
            self.log.info('{}'.format(app_submit_log))

            self.log.info('======> Ygg log execution:')
            # app_log = '\n'.join(app_execution_log)
            self.log.info('{}'.format(app_execution_log))
            
            self.log.info('======> Ygg log end.')

            if app_state is False:
                raise

        except Exception as e:
            self.log.error('Executing failed: %s', self.engineParams)
            raise

        self.log.info("Done.")


    def on_kill(self):
        if self.hook:
            self.hook.kill()

