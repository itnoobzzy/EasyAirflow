import logging
from datetime import datetime

from flask_restful import Resource, reqparse

import config
from endpoints.handlers.airflow_task_handlers import TaskHandlers
from endpoints.handlers.task_define_handlers import TaskDefineHandlers
from endpoints.models.dag_task_dep_model import DagTaskDependence
from endpoints.models.task_instance_model import EasyAirflowTaskInstance
from endpoints.models.taskinstance_next_model import TaskInstanceNext
from utils.response_safe import safe

logger = logging.getLogger(__name__)


class TaskInstanceResource(Resource):

    @safe
    def put(self, task_id):
        """
        重跑任务实例，如果当前任务有依赖的影子任务，影子任务也需要重跑
        :param task_id:
        :return:
        """
        parser = reqparse.RequestParser()
        parser.add_argument('dag_id', type=str)
        parser.add_argument('task_id', type=str)
        parser.add_argument('plan_execution_date', type=int)
        # 下游任务是否也需要重跑
        parser.add_argument('downstream', type=int)

        args = parser.parse_args()
        dag_id = args['dag_id']
        if task_id is None:
            task_id = args['task_id']
        plan_execution_date = datetime.fromtimestamp(args['plan_execution_date'] / 1000, tz=config.TIMEZONE)
        downstream = args['downstream']

        if task_id is None \
                or plan_execution_date is None:
            raise Exception("task_id or plan_execution_date  can not be None！")

        def add_external_ti(dag_id, task_id, execution_date):
            external_task_ids = TaskDefineHandlers.get_external_task_id(task_id)
            for external_task_id in external_task_ids:
                ti = EasyAirflowTaskInstance.get_ti_by_id(dag_id, external_task_id, execution_date)
                if ti:
                    ti_list.append(ti)

        ti = EasyAirflowTaskInstance.get_ti_by_id(dag_id, task_id, plan_execution_date)
        ti_list = []
        if ti:
            ti_list.append(ti)
        add_external_ti(dag_id, task_id, plan_execution_date)

        if downstream:
            downstream_task_ids = DagTaskDependence.get_all_downstream_recursive(dag_id=dag_id, upstream_task_id=task_id)
            logger.info("rerun task downstream_task_ids {}".format(downstream_task_ids))
            for downstream_task_id in downstream_task_ids:
                ti = EasyAirflowTaskInstance.get_ti_by_id(dag_id, downstream_task_id, plan_execution_date)
                ti_list.append(ti)
                add_external_ti(dag_id, downstream_task_id, plan_execution_date)

        TaskHandlers.clear_task_instances(ti_list, activate_dag_runs=True)
        return {"status": 200, "data": "success"}

    @safe
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('task_instances', type=dict, action='append')
        args = parser.parse_args()
        task_instances = args['task_instances']

        ti_list = []
        for task_instance in task_instances:
            task_instance_next = TaskInstanceNext.get_task_instance_next_by_task_plan(task_id=task_instance['task_id'],
                                                                                      next_execution_date=task_instance[
                                                                                          'plan_execution_date'])
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
