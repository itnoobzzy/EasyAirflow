# -*- coding: utf-8 -*-
#
import traceback
from airflow import DAG

# 支持的任务类型
from airflow_dag_template.config_json import get_dag_template_config


dag_id = "{}"

config = get_dag_template_config(dag_id)
dag_config = {
    "dag_id": config['dag']['dag_id'],
    "schedule": config['dag']['schedule_interval'],
    "start_date": config['dag']['start_date'],
    "end_date": config['dag']['end_date'],
    "catchup": config['dag']['catchup']
}

# Create the DAG
with DAG(**dag_config) as dag:
    # 创建的所有实例
    task_instances = {}

    # 获取所有 task 的配置
    task_all = config['tasks']

    # 先创建任务实例
    for task in task_all:
        type_operator = task['type_operator']
        task_id = task['task_id']
        private_params = task['params']
        task_params = {
            "task_id": task_id,
            "pool": task['pool'],
            "start_date": task['start_date'],
            "end_date": task['end_date'],
            "depends_on_past": task['depends_on_past'],
            "wait_for_downstream": task['wait_for_downstream'],
            "owner": task['owner'],
            "queue": task['queue'],
            "retries": task['retries'],
            "retry_delay": task['retry_delay'],
            "execution_timeout": task['execution_timeout'],
            "trigger_rule": task['trigger_rule']
        }
        if private_params:
            task_params.update(**private_params)
        task_tmp = type_operator(**task_params)
        task_instances[task_id] = task_tmp

    # 在创建dag间的依赖关系
    dag_task_depends = config['depends']
    for depend in dag_task_depends:
        try:
            task_id = depend['task_id']
            task_upstream_id = depend['upstream_task_id']
            task_tmp = task_instances[task_id]
            task_tmp.set_upstream(task_instances[task_upstream_id])
        except:
            # track
            traceback.print_exc()
            continue