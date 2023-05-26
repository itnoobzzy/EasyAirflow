# -*- coding: utf-8 -*-
#
import json
from typing import Any, Dict, List

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from plugins.operators.utils.encryption import str_decryption, str_encryption
from plugins.operators.utils.var_parse import VarParse
from datetime import datetime


class LandsatYggInvokeOperator(BaseOperator):
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
            app_type: str = '',
            ygg_task_id: str = None,
            data=None,
            ygg_conn_id: str = 'ygg_default',
            *args, **kwargs):
        super(LandsatYggInvokeOperator, self).__init__(*args, **kwargs)
        self.app_type = app_type
        self.ygg_task_id = ygg_task_id
        if data is not None and type(data) == str:
            data = data.encode("utf-8").decode("latin1")
        self.data = data or {}
        self.ygg_conn_id = ygg_conn_id
        self.hook = None
        self.is_product = kwargs.get("is_product", False)

    def _replace_time_str(self, task_params, context, is_product=False):
        if not is_product:
            data_json_str = json.dumps(task_params)
            complete_data_json_str = VarParse.operator_re_replace_datetime_var(data_json_str, context)
            new_task_params = json.loads(complete_data_json_str)
        else:
            new_task_params = []
            for param in task_params:
                param_list = param.split(":")
                decode_param = str_decryption(param_list[1])
                new_param = VarParse.operator_re_replace_datetime_var(decode_param, context)
                new_task_params.append(param_list[0] + ':' + str_encryption(new_param))
        return new_task_params

    def execute(self, context=None):
        self.log.info('Executing: %s', self.data)
        if "taskParams" in self.data:
            taskParams = self.data["taskParams"]
            is_product = self.data.get('is_product', False)
            self.data["taskParams"] = self._replace_time_str(taskParams, context, is_product=is_product)

        self.log.info('Executing really: %s', self.data)
        self.log.info('Executing: %s', self.task_id)

        try:
            from hooks.landsat_ygg_invoke_hook import LandsatYggInvokeHook
            self.hook = LandsatYggInvokeHook(ygg_conn_id=self.ygg_conn_id, context=context)

            if self.ygg_task_id is not None:
                app_state, app_submit_log, app_execution_log = self.hook.invoke_run(
                    task_id=self.ygg_task_id,
                    app_type=self.app_type,
                    data=self.data
                )
            else:
                app_state, app_submit_log, app_execution_log = self.hook.create_run(
                    app_type=self.app_type,
                    data=self.data
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
            self.log.error('Executing failed: %s', self.task_id)
            raise

        self.log.info("Done.")


    def on_kill(self):
        if self.hook:
            self.hook.kill(
                    app_type=self.app_type
            )
