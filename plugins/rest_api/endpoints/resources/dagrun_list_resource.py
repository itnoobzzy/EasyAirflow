import logging

from flask_restful import Resource, reqparse

from rest_api.endpoints.handlers.deg_define_handlers import DagDefineHandlers
from rest_api.utils.response_safe import safe

logger = logging.getLogger(__name__)


class DagRunListResource(Resource):
    @safe
    def get(self):

        parser = reqparse.RequestParser()
        parser.add_argument('page_size', type=int)
        parser.add_argument('page_num', type=int)
        parser.add_argument('order_by_field', type=str)
        parser.add_argument('reverse', type=int)
        parser.add_argument('dag_ids', type=str, action="append")
        parser.add_argument('states', type=str, action="append")
        parser.add_argument('min_plan_execution_date', type=int)
        parser.add_argument('max_plan_execution_date', type=int)

        args = parser.parse_args()
        page_size = args['page_size']
        page_num = args['page_num']
        order_by_field = args['order_by_field']
        reverse = args['reverse']
        dag_ids = args['dag_ids']
        states = args['states']
        min_plan_execution_date = args['min_plan_execution_date']
        max_plan_execution_date = args['max_plan_execution_date']

        count, dag_runs = DagDefineHandlers.get_dag_run(
            dag_ids,
            states,
            min_plan_execution_date,
            max_plan_execution_date,
            page_size,
            page_num,
            reverse,
            order_by_field
        )
        return {'status': 200, "data": {"count": count, "dag_runs": dag_runs}}
