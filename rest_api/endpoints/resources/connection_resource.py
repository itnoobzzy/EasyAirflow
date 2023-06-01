import logging

from flask_restful import Resource, reqparse

from client.landsat_client import airflow_client
from utils.exceptions import EasyAirflowExceptions
from utils.response_safe import safe

logger = logging.getLogger(__name__)


class ConnectionResource(Resource):
    @safe
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('airflow_conns', type=dict, action="append")
        # parser.add_argument('conn_type', type=str)
        # parser.add_argument('connection_id', type=str)
        # parser.add_argument('description', type=str)
        # parser.add_argument('host', type=str)
        # parser.add_argument('login', type=str)
        # parser.add_argument('port', type=int)
        # parser.add_argument('schema', type=str)
        # parser.add_argument('extra', type=str)
        # parser.add_argument('password', type=str)

        args = parser.parse_args()
        airflow_conns = args['airflow_conns']
        conns = []
        for airflow_conn in airflow_conns:
            data = {
                "conn_type": airflow_conn.get('conn_type'),
                "connection_id": airflow_conn.get('conn_id'),
                "description": airflow_conn.get('description'),
                "host": airflow_conn.get('host'),
                "login": airflow_conn.get('login'),
                "port": airflow_conn.get('port'),
                "schema": airflow_conn.get('schema'),
                "extra": airflow_conn.get('extra'),
                "password": airflow_conn.get('password')
                 }
            status, res = airflow_client.add_connections(**data)
            if status != 200:
                raise EasyAirflowExceptions(res)
            conns.append(res)
        return conns



