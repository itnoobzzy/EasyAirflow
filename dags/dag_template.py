# -*- coding: utf-8 -*-
#
import traceback
from airflow import DAG

# 支持的任务类型
from airflow_dag_template.config_json import get_dag_template_config


dag_id = '_Landsat-dag-6960502732205588480'

config = get_dag_template_config(dag_id)


# Create the DAG
with DAG(**config['dag']) as dag:
    # 创建的所有实例
    task_instances = {}

    # 获取所有 task 的配置
    task_all = config['tasks']

    # 先创建任务实例
    for task in task_all:
        type_operator = task['type_operator']
        task_id = task['task_id']

        task_tmp = type_operator(**task)
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