import logging

from flask_restful import Resource, reqparse

from rest_api.endpoints.models.dagrun_model import DagRun
from rest_api.endpoints.models.taskinstance_next_model import TaskInstanceNext
from rest_api.utils.response_safe import safe
from rest_api.utils.times import dt2ts

logger = logging.getLogger(__name__)

class DagRunTaskInstancesResource(Resource):
    @safe
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('dag_id', type=str)
        parser.add_argument('plan_execution_date', type=int)

        args = parser.parse_args()
        dag_id = args['dag_id']
        plan_execution_date = args['plan_execution_date']
        if dag_id is None or plan_execution_date is None:
            raise Exception("dag_id or plan_execution_date can not be None！")

        # 后去 execution_date 执行时间
        task_instance_next = TaskInstanceNext.get_task_instance_next_by_dag_plan(dag_id=dag_id,next_execution_date=plan_execution_date)

        task_instance_list = DagRun.get_dagrun_task_instances(dag_id=dag_id, execution_date_timestamp_ms=task_instance_next.execution_date)

        task_instances = []
        for task_instance in task_instance_list:

            task_id = task_instance.task_id
            execution_date = task_instance.execution_date
            task_instance_next = TaskInstanceNext.get_task_instance_next(task_id, execution_date)

            task_instance_dict = task_instance.props()
            task_instance_dict['plan_execution_date'] = int(dt2ts(task_instance_next.next_execution_date) * 1000)

            task_instances.append(task_instance_dict)

        response_data = {
            'status': 200,
            "data": {
                "dag_id": dag_id,
                "task_instances": task_instances
            }

        }
        http_status_code = 200
        return response_data, http_status_code

