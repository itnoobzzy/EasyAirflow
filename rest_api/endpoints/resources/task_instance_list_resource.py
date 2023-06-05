from datetime import datetime

from flask_restful import Resource, reqparse

from endpoints.handlers.airflow_task_handlers import TaskHandlers
from endpoints.handlers.task_define_handlers import TaskDefineHandlers
from endpoints.models.task_model import TaskDefine
from utils.response_safe import safe


class TaskInstanceListResource(Resource):

    @safe
    def get(self):

        parser = reqparse.RequestParser()
        parser.add_argument('dag_id', type=str)
        parser.add_argument('page_size', type=int)
        parser.add_argument('page_num', type=int)
        parser.add_argument('order_by_field', type=str)
        parser.add_argument('reverse', type=int)
        parser.add_argument('task_ids', type=str, action="append")
        parser.add_argument('states', type=str, action="append")
        parser.add_argument('instance_type', type=str)
        parser.add_argument('min_plan_execution_date', type=int)
        parser.add_argument('max_plan_execution_date', type=int)

        args = parser.parse_args()
        dag_id = args['dag_id']
        page_size = args['page_size']
        page_num = args['page_num']
        order_by_field = args['order_by_field']
        reverse = args['reverse']
        task_ids = args['task_ids']
        states = args['states']
        min_execution_date_timestamp = args['min_plan_execution_date']
        max_execution_date_timestamp = args['max_plan_execution_date']

        min_plan_execution_date = None
        max_plan_execution_date = None
        if isinstance(min_execution_date_timestamp, int):
            min_plan_execution_date = datetime.utcfromtimestamp(min_execution_date_timestamp / 1000)
        if isinstance(max_execution_date_timestamp, int):
            max_plan_execution_date = datetime.utcfromtimestamp(max_execution_date_timestamp / 1000)

        ti_info_dict_list, count_instance_info = TaskDefineHandlers.get_task_instance(
            dag_id,
            task_ids,
            states,
            min_plan_execution_date,
            max_plan_execution_date,
            page_size,
            page_num,
            reverse,
            order_by_field
        )

        return {'status': 200, "data": {"count": count_instance_info, "task_instances": ti_info_dict_list}}

    @safe
    def post(self):
        """
        任务补数
        :return:
        """
        parser = reqparse.RequestParser()
        parser.add_argument('task_id', type=str)
        parser.add_argument('start_plan_execution_date', type=int)
        parser.add_argument('end_plan_execution_date', type=int)

        args = parser.parse_args()
        task_id = args['task_id']
        start_plan_execution_date = args['start_plan_execution_date']
        end_plan_execution_date = args['end_plan_execution_date']
        if task_id is None \
                or start_plan_execution_date is None \
                or end_plan_execution_date is None:
            raise Exception("task_id or start_plan_execution_date or end_plan_execution_date can not be None！")

        timestamp_list = TaskDefine.get_backfill_task_plan_execution_date_timepstamp(
            start_plan_execution_date=start_plan_execution_date, end_plan_execution_date=end_plan_execution_date,
            task_id=task_id)

        if timestamp_list is not None:
            TaskHandlers.complement_task_instances(task_id, timestamp_list)

        response_data = {
            'status': 200,
            "data": {
                "plan_execution_dates": timestamp_list
            }

        }
        http_status_code = 200
        return response_data, http_status_code