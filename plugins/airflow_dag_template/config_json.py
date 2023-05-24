import copy
from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.external_task import ExternalTaskSensor

from airflow_dag_template.DagDefine import DagDefineModel
from airflow_dag_template.DagTaskDep import DagTaskDepModel
from airflow_dag_template.TaskDefine import TaskDefineModel
from airflow_dag_template.callback_funs import on_success_callback_fn, on_failure_callback_fn, on_retry_callback_fn, \
    sla_miss_callback_fn
from airflow_dag_template.external_task_sensor import landsat_execution_date_fn
from airflow_dag_template.sqlalchemy_util import props, provide_session

from operators.landsat_hive_operator import LandsatHiveOperator
from operators.landsat_http_operator import LandsatHttpOperator
from operators.landsat_livy_spark_batch_operator import LandsatLivySparkBatchOperator
from operators.landsat_livy_spark_sql_operator import LandsatLivySparkSQLOperator
from operators.landsat_mysql_operator import LandsatMySqlOperator
from operators.landsat_presto_operator import LandsatPrestoOperator
from operators.landsat_ygg_spark_operator import LandsatYggSparkOperator
from operators.landsat_ygg_invoke_operator import LandsatYggInvokeOperator
from sensors.landsat_external_task_sensor import LandsatExternalTaskSensor

key_operators = {
    'EmptyOperator': EmptyOperator,
    'BashOperator': BashOperator,
    'ExternalTaskSensor': ExternalTaskSensor,
    'MySqlOperator': MySqlOperator,
    'LandsatMySqlOperator': LandsatMySqlOperator,
    'LandsatHiveOperator': LandsatHiveOperator,
    'LandsatLivySparkSQLOperator': LandsatLivySparkSQLOperator,
    'LandsatPrestoOperator': LandsatPrestoOperator,
    'LandsatLivySparkBatchOperator': LandsatLivySparkBatchOperator,
    'LandsatHttpOperator': LandsatHttpOperator,
    'LandsatYggSparkOperator': LandsatYggSparkOperator,
    'LandsatYggInvokeOperator': LandsatYggInvokeOperator,
    'LandsatExternalTaskSensor': LandsatExternalTaskSensor,
}

# 需要支持的回调函数
key_callback_fns = {
    'landsat_execution_date_fn': landsat_execution_date_fn
}


def taskDefineModel_to_taskDefine(obj):
    obj_to_dict = props(obj)

    # 重试间隔
    retry_delay = timedelta(minutes=obj_to_dict['retry_delay_num_minutes'])
    obj_to_dict['retry_delay'] = retry_delay

    # todo 运行超时时间，需要在 表中增加字段,现在先写死
    execution_timeout = timedelta(minutes=obj_to_dict['execution_timeout_num_minutes'])
    # execution_timeout = timedelta(minutes=120)
    obj_to_dict['execution_timeout'] = execution_timeout

    # 私有参数
    private_params = copy.deepcopy(obj_to_dict['private_params'])
    obj_to_dict.pop('private_params', None)

    # 特殊数据类型才需要，这个和 任务类型有关
    if 'execution_delta_num_minutes' in private_params:
        execution_delta = timedelta(minutes=private_params['execution_delta_num_minutes'])
        private_params['execution_delta'] = execution_delta

    # 针对外部依赖使用的，计算依赖的函数
    if 'execution_date_fn' in private_params:
        key_callback_fn = private_params['execution_date_fn']

        execution_date_fn = key_callback_fns[key_callback_fn]
        private_params['execution_date_fn'] = execution_date_fn

    obj_to_dict.update(private_params)

    # 判断任务类型
    type_operator = obj_to_dict['operator']
    obj_to_dict['type_operator'] = key_operators[type_operator]

    return obj_to_dict


@provide_session
def get_dag_template_config(dag_id, session=None):
    # dag 和 task 的 配置信息
    config = {
        'dag': {},
        "tasks": [],
        "depends": []
    }

    # dag 配置
    dag_define = session.query(DagDefineModel) \
        .filter(DagDefineModel.dag_id == dag_id) \
        .filter(DagDefineModel.is_publish == True) \
        .first()

    if dag_define is None:
        raise Exception('{dag_id} not publish'.format(dag_id=dag_id))

    config['dag'] = dag_define.get_obj_dict()

    # 添加 default arg
    """
            'default_args':{
            'owner': 'wanglong',
            'start_date':'2019-01-01T00:00:00Z',
            'on_success_callback':on_success_callback_fn
        }
    """
    config['dag']['default_args'] = {
        'on_failure_callback': on_failure_callback_fn,
        'on_success_callback': on_success_callback_fn,
        'on_retry_callback': on_retry_callback_fn,
        'sla_miss_callback': sla_miss_callback_fn,

        # 'execution_timeout': timedelta(minutes=120),
    }

    """
    获取 dag 中 start_date 的最小值，end_date 的最大值
    """
    from sqlalchemy.sql import func
    qry = session.query(func.min(TaskDefineModel.start_date).label("min_start_date"),
                        func.max(TaskDefineModel.end_date).label("max_end_date"),
                        ) \
        .filter(TaskDefineModel.dag_id == dag_id) \
        .filter(TaskDefineModel.is_publish == True)
    res = qry.one()
    min_start_date = res.min_start_date
    max_end_date = res.max_end_date
    config['dag']['start_date'] = min_start_date
    config['dag']['end_date'] = max_end_date

    # task 配置
    task_define_list = session.query(TaskDefineModel) \
        .filter(TaskDefineModel.dag_id == dag_id) \
        .filter(TaskDefineModel.is_publish == True)

    for res_value in task_define_list:
        task_config = taskDefineModel_to_taskDefine(res_value)
        config['tasks'].append(task_config)

    # 依赖配置
    dag_task_dep_list = session.query(DagTaskDepModel) \
        .filter(DagTaskDepModel.dag_id == dag_id) \
        .filter(DagTaskDepModel.type == 2) \

    for res_value in dag_task_dep_list:
        dag_task_dep_config = res_value.get_obj_dict()
        config['depends'].append(dag_task_dep_config)

    return config


if __name__ == '__main__':
    dag_id = 'dag-Landsat_LandsatMySqlOperator__LTAdmin_1603179073040'
    config = get_dag_template_config(dag_id)
    print(config)
