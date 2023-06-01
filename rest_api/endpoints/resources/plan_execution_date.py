import logging

from flask_restful import Resource, reqparse, request

from endpoints.models.dagrun_model import DagRun
from utils.response_safe import safe
from utils.times import dt2ts

logger = logging.getLogger(__name__)

class DagRunPlanExecutionDateResource(Resource):
    @safe
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('dag_id', type=str)
        parser.add_argument('limit_num', type=int)

        args = parser.parse_args()
        dag_id = args['dag_id']
        limit_num = args['limit_num']
        if dag_id is None or limit_num is None:
            raise Exception("dag_id or limit_num can not be None！")

        # 通过查询 dag run 表获取最近 限制的条数
        dagruns = DagRun.get_dagrun_plan_execution_date(dag_id=dag_id, limit_num=limit_num)

        plan_execution_dates = []
        for dagrun in dagruns:
            plan_execution_date = {
                'plan_execution_date' : int(dt2ts(dagrun.next_execution_date) * 1000),
                'execution_date' : int(dt2ts(dagrun.execution_date) * 1000)
            }

            plan_execution_dates.append(plan_execution_date)

        response_data = {
            'status': 200,
            "data": {
                "dag_id": dag_id,
                "plan_execution_dates": plan_execution_dates
            }

        }
        http_status_code = 200
        return response_data, http_status_code







