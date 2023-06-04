from datetime import datetime

from flask_restful import Resource, reqparse

from endpoints.handlers.task_define_handlers import TaskDefineHandlers
from endpoints.models.taskinstance_type_model import TaskInstanceType
from utils.response_safe import safe


class TaskInstanceSummaryResource(Resource):

    @safe
    def get(self):

        parser = reqparse.RequestParser()
        parser.add_argument('dag_id', type=str)
        parser.add_argument('task_ids', type=str, action="append")
        parser.add_argument('states', type=str, action="append")
        parser.add_argument('instance_type', type=str)
        parser.add_argument('min_plan_execution_date', type=int)
        parser.add_argument('max_plan_execution_date', type=int)

        args = parser.parse_args()

        dag_id = args['dag_id']
        task_ids = args['task_ids']
        states = args['states']
        min_execution_date_timestamp = args['min_plan_execution_date']
        max_execution_date_timestamp = args['max_plan_execution_date']

        state_num = TaskDefineHandlers.get_task_instance_summary(
            dag_id,
            task_ids,
            states,
            min_execution_date_timestamp,
            max_execution_date_timestamp
        )

        return {'status': 200, "data": state_num}
