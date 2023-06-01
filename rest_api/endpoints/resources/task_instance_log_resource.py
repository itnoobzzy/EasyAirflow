import logging

from flask_restful import Resource, reqparse, request

from endpoints.handlers.task_define_handlers import TaskDefineHandlers
from endpoints.models.dagrun_model import DagRun
from endpoints.models.taskinstance_next_model import TaskInstanceNext
from utils.response_safe import safe
from utils.times import dt2ts

logger = logging.getLogger(__name__)

class TaskInstanceLogResource(Resource):
    @safe
    def get(self,task_id):
        parser = reqparse.RequestParser()
        parser.add_argument('task_id', type=str)
        parser.add_argument('plan_execution_date', type=int)
        parser.add_argument('try_num', type=int)

        args = parser.parse_args()
        if task_id is None:
            task_id = args['task_id']
        plan_execution_date = args['plan_execution_date']
        try_num = args['try_num']
        if task_id is None \
                or plan_execution_date is None:
            raise Exception("task_id or plan_execution_date can not be None！")


        task_instance_next = TaskInstanceNext.get_task_instance_next_by_task_plan(task_id=task_id,next_execution_date=plan_execution_date)
        execution_date = task_instance_next.execution_date
        dag_id = task_instance_next.dag_id

        item = TaskDefineHandlers.get_task_instance_try_log(dag_id, task_id, execution_date, try_num)
        response_data = {
            'status': 200,
            "data": item

        }
        http_status_code = 200
        return response_data, http_status_code



