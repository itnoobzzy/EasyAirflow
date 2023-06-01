import json
import logging

import requests

import config
from utils.error_enum import ErrorEnum

logger = logging.getLogger(__name__)


class AirflowClient:
    """
    访问 airflow api
    """

    def __init__(self, airflow_host):
        self.add_connections_url = 'POST', airflow_host + '/api/v1/connections'

    def do_request(self,
                   url,
                   method,
                   headers=None,
                   params=None,
                   formdata=None,
                   jsondata=None):

        request_param = {
            "auth": (config.AIRFLOW_USER, config.AIRFLOW_PWD)
        }
        if params:
            request_param['params'] = params
        if formdata:
            request_param['data'] = formdata
        if jsondata:
            request_param['json'] = jsondata
        if method == 'DELETE':
            res = requests.delete(url, headers=headers, timeout=20, params=params)
        elif method == 'GET':
            res = requests.get(url, headers=headers, timeout=20, params=params)
        elif method == 'POST':
            res = requests.post(url, headers=headers, timeout=60, **request_param)
        elif method == 'PUT':
            res = requests.put(url, headers=headers, timeout=60, **request_param)
        else:
            raise Exception('Method not allowed: {}'.format(method))
        http_code = res.status_code
        res_text = res.text
        logger.info(f'request airflow {method} {url}')
        logger.info(f'request airflow headers {json.dumps(headers)}')
        logger.info(f'request airflow param {json.dumps(request_param)}')
        logger.info(f'airflow server return code {http_code}')
        logger.info(f'airflow server return text {res_text}')
        if http_code != 200:
            return ErrorEnum.AIRFLOW_ERROR.value
        return http_code, json.loads(res_text)

    def add_connections(self, **kwargs):
        status, res_data = self.do_request(
            method=self.add_connections_url[0],
            url=self.add_connections_url[1],
            jsondata=kwargs)
        return status, res_data


airflow_client = AirflowClient(config.AIRFLOW_HOST)
