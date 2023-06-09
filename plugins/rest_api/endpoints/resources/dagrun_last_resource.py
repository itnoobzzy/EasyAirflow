import logging
from datetime import datetime

from flask_restful import Resource, reqparse

from rest_api.endpoints.handlers.deg_define_handlers import DagDefineHandlers
from rest_api.utils.response_safe import safe


logger = logging.getLogger(__name__)

class DagRunLastResource(Resource):
    # todo
    @safe
    def get(self):

        parser = reqparse.RequestParser()
        parser.add_argument('page_size', type=int)
        parser.add_argument('page_num', type=int)
        parser.add_argument('order_by_field', type=str)
        parser.add_argument('reverse', type=int)
        parser.add_argument('dag_ids',type=str,action="append")
        parser.add_argument('states',type=str,action="append")
        parser.add_argument('min_plan_execution_date', type=int)
        parser.add_argument('max_plan_execution_date', type=int)

        args = parser.parse_args()
        page_size = args['page_size']
        if page_size is None:
            page_size = 2
        page_num = args['page_num']
        if page_num is None:
            page_num = 1
        order_by_field = args['order_by_field']
        if order_by_field is None:
            order_by_field = 'next_execution_date'
        reverse = args['reverse']
        if reverse is None:
            reverse = 1
        dag_ids = args['dag_ids']
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

        all_ti_info_dict_list = []
        for dag_id in dag_ids:
            dag_ids_tmp = [dag_id]
            ti_info_dict_list = DagDefineHandlers.get_dag_run_latest(
                dag_ids_tmp,
                states,
                min_plan_execution_date,
                max_plan_execution_date,
                page_size,
                page_num,
                reverse,
                order_by_field
            )
            all_ti_info_dict_list.extend(ti_info_dict_list)

        response_data = {
            'status': 200,
            "data": {
                "dag_runs": all_ti_info_dict_list
            }

        }
        http_status_code = 200
        return response_data, http_status_code









