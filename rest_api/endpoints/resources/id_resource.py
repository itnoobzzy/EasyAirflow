import logging

from flask_restful import Resource, reqparse

from utils.response_safe import safe
from utils.sonwflakeId import time_genector


logger = logging.getLogger(__name__)


class IdResource(Resource):
    @safe
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('type', type=str)
        parser.add_argument('domain', type=str)

        args = parser.parse_args()
        type = args['type']
        domain = args['domain']
        if type is None or domain is None:
            raise Exception("type or domain can not be None！")

        snowflake_id = time_genector.nextID()

        airflow_id = '_{domain}-{type}-{snowflake_id}'.format(
            domain=domain,
            type=type,
            snowflake_id=snowflake_id
        )
        logger.info('airflow id:{airflow_id}'.format(airflow_id=airflow_id))
        response_data = {
            'status': 200,
            "data": {
                "airflow_id": airflow_id,
                "type": type
            }
        }
        http_status_code = 200
        return response_data, http_status_code
