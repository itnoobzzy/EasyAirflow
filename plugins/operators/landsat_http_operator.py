# -*- coding: utf-8 -*-
#
import json

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from hooks.landsat_http_hook import LandsatHttpHook
from operators.utils.var_parse import VarParse


class LandsatHttpOperator(BaseOperator):
    """
    Calls an endpoint on an HTTP system to execute an action

    :param http_conn_id: The connection to run the operator against
    :type http_conn_id: str
    :param endpoint: The relative part of the full url. (templated)
    :type endpoint: str
    :param method: The HTTP method to use, default = "POST"
    :type method: str
    :param data: The data to pass. POST-data in POST/PUT and params
        in the URL for a GET request. (templated)
    :type data: For POST/PUT, depends on the content-type parameter,
        for GET a dictionary of key/value string pairs
    :param headers: The HTTP headers to be added to the GET request
    :type headers: a dictionary of string key/value pairs
    :param response_check: A check against the 'requests' response object.
        Returns True for 'pass' and False otherwise.
    :type response_check: A lambda or defined function.
    :param extra_options: Extra options for the 'requests' library, see the
        'requests' documentation (options to modify timeout, ssl, etc.)
    :type extra_options: A dictionary of options, where key is string and value
        depends on the option that's being modified.
    :param xcom_push: Push the response to Xcom (default: False).
        If xcom_push is True, response of an HTTP request will also
        be pushed to an XCom.
    :type xcom_push: bool
    :param log_response: Log the response (default: False)
    :type log_response: bool
    """

    # template_fields = ['endpoint', 'data', 'headers', ]
    # template_ext = ()
    # ui_color = '#f4a460'

    """
    header 需要指明 
    headers = {'Content-Type': 'application/json'}
    headers = {'token': 'token-value'}
    """

    @apply_defaults
    def __init__(self,
                 endpoint,
                 method='POST',
                 data=None,
                 headers=None,
                 response_check=None,
                 extra_options=None,
                 http_conn_id='http_default',
                 log_response=False,
                 *args, **kwargs):
        super(LandsatHttpOperator, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.method = method
        self.endpoint = endpoint
        self.headers = headers or {}

        self.response_check = response_check
        self.extra_options = extra_options or {}
        self.log_response = log_response

        if data is not None and type(data) == str:
            data = data.encode("utf-8").decode("latin1")
            
        self.data = data or {}

    def execute(self, context):

        self.log.info('headers: %s', self.headers)
        self.log.info('headers type: %s', type(self.headers))
        if type(self.headers) == str:
            self.headers = json.loads(self.headers)

        self.log.info('headers: %s', self.headers)
        self.log.info('headers type: %s', type(self.headers))

        self.log.info('Executing: %s', self.data)

        data_json_str = json.dumps(self.data)
        complete_data_json_str = VarParse.operator_re_replace_datetime_var(data_json_str, context)
        self.data = json.loads(complete_data_json_str)

        self.log.info('Executing really: %s', self.data)


        http = LandsatHttpHook(self.method, http_conn_id=self.http_conn_id)

        self.log.info("Calling HTTP method")

        response = http.run(self.endpoint,
                            self.data,
                            self.headers,
                            self.extra_options)
        if self.log_response:
            self.log.info(response.text)
        if self.response_check:
            if not self.response_check(response):
                raise AirflowException("Response check returned False.")

