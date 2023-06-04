import logging
from datetime import datetime

from flask_restful import Resource, reqparse

from endpoints.models.dagrun_model import DagRun
from endpoints.models.dagrun_next_model import DagRunNext
from utils.response_safe import safe


logger = logging.getLogger(__name__)


class DagRunResource(Resource):
    @safe
    def get(self):

        parser = reqparse.RequestParser()
        parser.add_argument('dag_id', type=str)
        parser.add_argument('execution_date', type=int)


        args = parser.parse_args()

        dag_id = args['dag_id']
        execution_date_timestamp = args['execution_date']


        execution_date = None
        if isinstance(execution_date_timestamp, int):
            execution_date = datetime.utcfromtimestamp(execution_date_timestamp / 1000)

        dag_run = DagRun.get_dag_run_by_dag_id_execution_date(
            dag_id,
            execution_date

        )
        dag_run_dict = dag_run.props()

        dag_run_next = DagRunNext.get_dag_run_next_by_dag_id_execution_date(
            dag_id,
            execution_date
        )
        dag_run_next_dict = dag_run_next.props()

        dag_run_dict["plan_execution_date"] = dag_run_next_dict["next_execution_date"]

        response_data = {
            'status': 200,
            "data": dag_run_dict

        }
        http_status_code = 200
        return response_data, http_status_code









