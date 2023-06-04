import logging

from flask_restful import Resource, reqparse

from endpoints.models.dag_model import DagDefine
from endpoints.models.dag_task_dep_model import DagTaskDependence
from endpoints.models.task_model import TaskDefine
from config import DAGS_DIR
from utils.airflow_web_dag_handlers import DagHandlers
from utils.database import db
from utils.exceptions import DagsException
from utils.response_safe import safe

logger = logging.getLogger(__name__)


class DagResource(Resource):

    @staticmethod
    def _cancel_task(dag_id):
        """下线任务并删除任务间依赖"""
        task_defines = TaskDefine.get_tasks_by_dag_id(dag_id)
        for task_define in task_defines:
            task_id = task_define.task_id
            TaskDefine.cancel_publish_task(task_id)
            DagTaskDependence.delete_dep_by_task_id(task_id)

    @staticmethod
    def _save_dag_info(dag_id, schedule_interval):
        dag_define = DagDefine(
            dag_id=dag_id,
            schedule_interval=schedule_interval,
            is_publish=True
        )
        dag_define.upsert()

    @staticmethod
    def _save_task_info(tasks, dag_id, schedule_interval):
        task_defines = []
        for task in tasks:
            task_id = task['task_id']
            owner = task['owner']
            operator = task['operator']
            start_date = task['start_date']
            end_date = task['end_date']
            retries = task['retries']
            retry_delay_num_minutes = task['retry_delay_num_minutes']
            execution_timeout_num_minutes = task['execution_timeout_num_minutes']
            pool = task['pool']
            queue = task['queue']
            priority_weight = task['priority_weight']
            private_params = task['private_params']
            start_date, end_date = TaskDefine.set_task_start_end_datetime(start_date, end_date, schedule_interval)
            task_define = TaskDefine(
                schedule_interval=schedule_interval,
                task_id=task_id,
                operator=operator,
                private_params=private_params,
                dag_id=dag_id,
                owner=owner,
                is_publish=True,
                start_date=start_date,
                end_date=end_date,
                retries=retries,
                retry_delay_num_minutes=retry_delay_num_minutes,
                execution_timeout_num_minutes=execution_timeout_num_minutes,
                pool=pool,
                queue=queue,
                priority_weight=priority_weight
            )
            task_define.upsert()
            task_defines.append(task_define)
        for task_define in task_defines:
            # 检查依赖选项,添加依赖
            TaskDefine.task_add_dep_by_user_dep(task_define)

    @staticmethod
    def _save_task_depends(task_depends, schedule_interval):
        logger.info("publish dag task_depends {}".format(task_depends))
        for task_depend in task_depends:
            task_id = task_depend['task_id']
            dag_id = task_depend['dag_id']
            upstream_task_id = task_depend['upstream_task_id']
            trigger_rule = task_depend['trigger_rule']

            if dag_id is None or schedule_interval is None or upstream_task_id is None or trigger_rule is None:
                raise DagsException("dag_id or schedule_interval can not be None！")

            dag_task_dep = DagTaskDependence(
                dag_id=dag_id,
                task_id=task_id,
                upstream_task_id=upstream_task_id,
                trigger_rule=trigger_rule,
                type=DagTaskDependence.USER_DEP_TYPE
            )
            dag_task_dep.upsert()

    @staticmethod
    def _sync_dag_file(dag_id):
        """
        同步dag文件到airflow的dag目录下
        如果 airflow 是 docker 部署，需要将文件同步到 docker 容器挂载的目录下
        如果 airflow 是 k8s 部署，需要将文件同步到 k8s 的挂载目录下
        :param dag_id:
        :return:
        """
        DagDefine.publish_dag_file(dag_id)

    @safe
    def post(self):
        """
        发布 DAG：
        1. 先显现该 DAG 下所有的任务以及任务依赖
        2. 更新 dag 信息，任务信息， 依赖任务信息
        3. 同步 dag 文件到 scheduler 扫描的 DAG 目录下
        :return:
        """
        parser = reqparse.RequestParser()
        parser.add_argument('dag', type=dict)
        parser.add_argument('tasks', type=dict, action="append")
        parser.add_argument('task_depends', type=dict, action="append")

        logger.info("publish dag {}".format(parser))

        args = parser.parse_args()
        dag = args['dag']
        tasks = args['tasks'] or []
        task_depends = args['task_depends'] or []

        dag_id = dag['dag_id']
        schedule_interval = dag['schedule_interval']

        if dag_id is None or schedule_interval is None:
            raise DagsException("dag_id or schedule_interval can not be None！")

        self._cancel_task(dag_id)
        self._save_dag_info(dag_id, schedule_interval)
        self._save_task_depends(task_depends, schedule_interval)
        self._save_task_info(tasks, dag_id, schedule_interval)
        self._sync_dag_file(dag_id)

        return {
            'status': 200,
            "data": {
                "dag": dag,
                "tasks": tasks,
                "task_depends": task_depends,
            }
        }

    @safe
    def delete(self):
        """
        撤销发布 DAG
        1. 删除 dag 信息
        2. 下线 dag 下所有的任务及影子任务
        """
        parser = reqparse.RequestParser()
        parser.add_argument('dag_id', type=str)

        args = parser.parse_args()
        dag_id = args['dag_id']

        DagDefine.delete_dag_by_dag_id(dag_id=dag_id)
        task_defines = TaskDefine.get_tasks_by_dag_id(dag_id)
        for task_define in task_defines:
            task_id = task_define.task_id
            TaskDefine.cancel_publish_task(task_id)

        return {'status': 200, "data": {"dag_id": dag_id}}


class DagFile(Resource):

    @safe
    def delete(self):
        """删除dag文件"""
        parser = reqparse.RequestParser()
        parser.add_argument('dag_id', type=str)
        args = parser.parse_args()
        dag_id = args['dag_id']

        dag_define = DagDefine.get_dag(dag_id=dag_id)
        if dag_define is not None:
            dag_define.is_publish = False
            dag_define.deletion = 1
            dag_define.upsert()

        logger.info("delete dag {}".format(dag_id))
        err = DagHandlers.delete_dag_local(airflow_dag_base_dir=DAGS_DIR, dag_id=dag_id)
        response_data = {
            'status': 200,
            'info': 'success'
        }
        if err:
            response_data['status'] = 404
            response_data['info'] = err
        return response_data, 200

    @safe
    def post(self):
        """上传dag文件"""
        parser = reqparse.RequestParser()
        parser.add_argument('dag_id', type=str)

        args = parser.parse_args()
        dag_id = args['dag_id']

        response_data = {
            'status': 200,
            'info': 'success'
        }
        try:
            DagHandlers.upload_dag_local(airflow_dag_base_dir=DAGS_DIR, dag_id=dag_id)
        except Exception as e:
            response_data['status'] = 500
            logger.exception(f"上传dag文件失败：{e}")
            response_data['info'] = "上传dag文件失败"
        finally:
            return response_data, 200
