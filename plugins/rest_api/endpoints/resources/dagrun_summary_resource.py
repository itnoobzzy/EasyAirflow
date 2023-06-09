from datetime import datetime

from flask_restful import Resource, reqparse

from rest_api.endpoints.handlers.deg_define_handlers import DagDefineHandlers
from rest_api.utils.response_safe import safe


class DagRunSummaryResource(Resource):

    # todo
    @safe
    def get(self):

        parser = reqparse.RequestParser()

        parser.add_argument('dag_ids',type=str,action="append")
        parser.add_argument('states',type=str,action="append")
        parser.add_argument('min_plan_execution_date', type=int)
        parser.add_argument('max_plan_execution_date', type=int)

        args = parser.parse_args()

        task_ids = args['dag_ids']
        states = args['states']
        min_execution_date_timestamp = args['min_plan_execution_date']
        max_execution_date_timestamp = args['max_plan_execution_date']

        # 将 NULL 转换为 None,统一为运行时状态
        if states and 'NULL' in states :
            states.append(None)
            states.remove('NULL')

        # if task_id is None \
        #         or start_plan_execution_date is None \
        #         or end_plan_execution_date is None:
        #     raise Exception("task_id or start_plan_execution_date or end_plan_execution_date can not be None！")

        min_plan_execution_date = None
        max_plan_execution_date = None
        if isinstance(min_execution_date_timestamp, int):
            min_plan_execution_date = datetime.utcfromtimestamp(min_execution_date_timestamp / 1000)
        if isinstance(max_execution_date_timestamp, int):
            max_plan_execution_date = datetime.utcfromtimestamp(max_execution_date_timestamp / 1000)

        state_num = DagDefineHandlers.get_dag_run_summary(
            task_ids,
            states,
            min_plan_execution_date,
            max_plan_execution_date,
        )

        response_data = {
            'status': 200,
            "data": state_num

        }
        http_status_code = 200
        return response_data, http_status_code

    pass
