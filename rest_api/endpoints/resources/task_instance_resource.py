import logging
from datetime import datetime, timedelta

from croniter import croniter
from flask_restful import Resource, reqparse, request

from endpoints.handlers.airflow_task_handlers import TaskHandlers
from endpoints.handlers.task_define_handlers import TaskDefineHandlers
from endpoints.models.dag_task_dep_model import DagTaskDependence
from endpoints.models.dagrun_model import DagRun
from endpoints.models.task_instance_model import TaskInstance
from endpoints.models.task_model import TaskDefine
from endpoints.models.taskinstance_next_model import TaskInstanceNext
from endpoints.models.taskinstance_type_model import TaskInstanceType
from utils.response_safe import safe
from utils.times import dt2ts

logger = logging.getLogger(__name__)


class TaskInstanceResource(Resource):

    @safe
    def get(self,task_id):
        """
        获取实例的具体信息，通过 执行时间 和 计划执行时间都可以获取
        通过 execution_date 获取的方式不是很好，不解耦
        :return:
        """
        parser = reqparse.RequestParser()
        parser.add_argument('task_id', type=str)
        parser.add_argument('plan_execution_date', type=int)
        parser.add_argument('execution_date', type=int)

        args = parser.parse_args()
        if task_id is None:
            task_id = args['task_id']
        plan_execution_date = args['plan_execution_date']

        if plan_execution_date :

            if task_id is None \
                    or plan_execution_date is None:
                raise Exception("task_id or plan_execution_date  can not be None！")


            task_instance_next = TaskInstanceNext.get_task_instance_next_by_task_plan(task_id=task_id,next_execution_date=plan_execution_date)

        else:
            execution_date = args['execution_date']

            if task_id is None \
                    or execution_date is None:
                raise Exception("task_id or execution_date  can not be None！")

            if isinstance(execution_date, int):
                execution_date = datetime.utcfromtimestamp(execution_date / 1000)

            task_instance_next = TaskInstanceNext.get_task_instance_next(task_id=task_id,execution_date=execution_date)

        # 获取需要的信息
        next_execution_date = task_instance_next.next_execution_date
        execution_date = task_instance_next.execution_date
        dag_id = task_instance_next.dag_id
        ti = TaskInstance.get_task_instance(dag_id, task_id, execution_date)
        ti_json = ti.props()
        ti_json['next_execution_date'] = int(dt2ts(next_execution_date) * 1000)

        response_data = {
            'status': 200,
            "data": {
                "task_instance": ti_json
            }

        }
        http_status_code = 200
        return response_data, http_status_code

    @safe
    def delete(self,task_id):
        """
        杀死正在运行的任务
        :return:
        """
        parser = reqparse.RequestParser()
        parser.add_argument('task_id', type=str)
        parser.add_argument('plan_execution_date', type=int)


        args = parser.parse_args()
        if task_id is None:
            task_id = args['task_id']
        plan_execution_date = args['plan_execution_date']

        if task_id is None \
                or plan_execution_date is None:
            raise Exception("task_id or plan_execution_date  can not be None！")


        task_instance_next = TaskInstanceNext.get_task_instance_next_by_task_plan(task_id=task_id,next_execution_date=plan_execution_date)
        execution_date = task_instance_next.execution_date
        dag_id = task_instance_next.dag_id

        ti = TaskInstance.get_task_instance(dag_id, task_id, execution_date)
        TaskHandlers.stop_task_instances([ti], activate_dag_runs=False)


        response_data = {
            'status': 200,
            "data": {

            }

        }
        http_status_code = 200
        return response_data, http_status_code


    @safe
    def put(self,task_id):
        parser = reqparse.RequestParser()
        parser.add_argument('task_id', type=str)
        parser.add_argument('plan_execution_date', type=int)
        parser.add_argument('downstream', type=str)


        args = parser.parse_args()
        if task_id is None:
            task_id = args['task_id']
        plan_execution_date = args['plan_execution_date']
        downstream = args['downstream']
        if downstream is not None and downstream.strip().lower() == 'true' :
            downstream = True
        else:
            downstream = False

        if task_id is None \
                or plan_execution_date is None:
            raise Exception("task_id or plan_execution_date  can not be None！")


        task_instance_next = TaskInstanceNext.get_task_instance_next_by_task_plan(task_id=task_id,next_execution_date=plan_execution_date)
        execution_date = task_instance_next.execution_date
        dag_id = task_instance_next.dag_id


        ti = TaskInstance.get_task_instance(dag_id, task_id, execution_date)
        ti_list = []
        ti_list.append(ti)
        if downstream :
            downstream_task_ids = DagTaskDependence.get_all_downstream_recursive(dag_id=dag_id,upstream_task_id=task_id)

            logger.info("rerun task downstream_task_ids {}".format(downstream_task_ids))
            for downstream_task_id in downstream_task_ids:
                ti = TaskInstance.get_task_instance(dag_id, downstream_task_id, execution_date)
                ti_list.append(ti)

                # 获取外部依赖的影子任务
                external_task_ids = TaskDefineHandlers.get_external_task_id(downstream_task_id)
                if external_task_ids:
                    for external_task_id in external_task_ids:
                        ti = TaskInstance.get_task_instance(dag_id, external_task_id, execution_date)
                        ti_list.append(ti)

        # 获取外部依赖的影子任务
        external_task_ids = TaskDefineHandlers.get_external_task_id(task_id)
        if external_task_ids:
            for external_task_id in external_task_ids:
                ti = TaskInstance.get_task_instance(dag_id, external_task_id, execution_date)
                ti_list.append(ti)

        TaskHandlers.clear_task_instances(ti_list, activate_dag_runs=True)

        response_data = {
            'status': 200,
            "data": {

            }

        }
        http_status_code = 200
        return response_data, http_status_code

    @safe
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('task_instances' ,type=dict, action='append')
        args = parser.parse_args()
        task_instances = args['task_instances']

        ti_list = []
        for task_instance in task_instances:
            task_instance_next = TaskInstanceNext.get_task_instance_next_by_task_plan(task_id=task_instance['task_id'],
                                                                                      next_execution_date=task_instance['plan_execution_date'])
            execution_date = task_instance_next.execution_date
            dag_id = task_instance_next.dag_id
            ti = TaskInstance.get_task_instance(dag_id, task_instance['task_id'], execution_date)

            external_task_ids = TaskDefineHandlers.get_external_task_id(task_instance['task_id'])
            if external_task_ids:
                for external_task_id in external_task_ids:
                    external_ti = TaskInstance.get_task_instance(dag_id, external_task_id, execution_date)
                    if external_ti:
                        ti_list.append(external_ti)
            if ti:
                ti_list.append(ti)

        TaskHandlers.clear_task_instances(ti_list, activate_dag_runs=True)

        response_data = {
            'status': 200,
            "data": {

            }

        }
        http_status_code = 200
        return response_data, http_status_code