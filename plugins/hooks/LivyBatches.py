import logging
import time
from enum import Enum
from typing import Any, Dict, List, Iterable, Iterator, Optional
from typing import Union, Tuple

import requests
from requests import HTTPError
from requests.auth import HTTPBasicAuth

Auth = Union[requests.auth.AuthBase, Tuple[str, str]]
Verify = Union[bool, str]

LOGGER = logging.getLogger(__name__)


class JsonClient:
    """A wrapper for a requests session for JSON formatted requests.

    This client handles appending endpoints on to a common hostname,
    deserialising the response as JSON and raising an exception when an error
    HTTP code is received.
    """

    def __init__(
            self, url: str, auth: Auth = None, verify: Verify = True
    ) -> None:
        self.url = url
        self.session = requests.Session()
        if auth is not None:
            self.session.auth = auth
        self.session.verify = verify

    def close(self) -> None:
        self.session.close()

    def get(self, endpoint: str = "") -> dict:
        return self._request("GET", endpoint)

    def post(self, endpoint: str, data: dict = None) -> dict:
        return self._request("POST", endpoint, data)

    def delete(self, endpoint: str = "") -> dict:
        return self._request("DELETE", endpoint)

    def my_raise_for_status(self, response):
        msg = response.text
        content = response.content
        status_code = response.status_code
        url = response.url

        http_error_msg = ''
        if isinstance(content, bytes):
            # We attempt to decode utf-8 first because some servers
            # choose to localize their reason strings. If the string
            # isn't utf-8, we fall back to iso-8859-1 for all other
            # encodings. (See PR #3538)
            try:
                reason = content.decode('utf-8')
            except UnicodeDecodeError:
                reason = content.decode('iso-8859-1')
        else:
            reason = content

        if 400 <= status_code < 500:
            http_error_msg = u'%s Client Error: %s for url: %s >>msg : %s >>reason : %s' % (
            status_code, reason, url, msg, reason)

        elif 500 <= status_code < 600:
            http_error_msg = u'%s Server Error: %s for url: %s >>msg : %s >>reason : %s' % (
            status_code, reason, url, msg, reason)

        if http_error_msg:
            raise HTTPError(http_error_msg, response=response)

    def _request(self, method: str, endpoint: str, data: dict = None) -> dict:
        url = self.url.rstrip("/") + endpoint
        response = self.session.request(method, url, json=data)
        self.my_raise_for_status(response)
        return response.json()


class LivyBatches(object):

    def __init__(
            self, url: str, auth: Auth = None, verify: Verify = True
    ) -> None:
        self._client = JsonClient(url, auth, verify)

    def close(self) -> None:
        """Close the underlying requests session."""
        self._client.close()

    def post_batches(self, data):
        response = self._client.post(
            f"/batches", data=data
        )
        LOGGER.info('post_batches response : {} '.format(response))
        batchId = response['id']
        state = response['state']
        return batchId, state

    def get_batches(self, batchId):
        response = self._client.get(
            f"/batches/{batchId}"
        )
        state = response['state']
        return state

    def get_batches_response(self, batchId):
        """
        get_batches response : {'id': 240, 'name': 'com.akulaku.spur.hivewithjdbc.entrance.JDBC2Hive#wanglong#1602470783.557297', 'owner': 'huhh', 'proxyUser': 'hive', 'state': 'running', 'appId': 'application_1599449904081_2743', 'appInfo': {'driverLogUrl': 'http://cdh108.akulaku.com:8042/node/containerlogs/container_1599449904081_2743_01_000001/hive', 'sparkUiUrl': 'http://cdh109.akulaku.com:8088/proxy/application_1599449904081_2743/'}, 'log': ['\t queue: root.users.hive', '\t start time: 1602470802467', '\t final status: UNDEFINED', '\t tracking URL: http://cdh109.akulaku.com:8088/proxy/application_1599449904081_2743/', '\t user: hive', '20/10/12 02:46:42 INFO util.ShutdownHookManager: Shutdown hook called', '20/10/12 02:46:42 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-32bdc131-9462-4f2b-a0f6-afca0fd6c0d7', '20/10/12 02:46:42 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-a67fa5fb-d725-497d-94a9-9e486769cfbd', '\nstderr: ', '\nYARN Diagnostics: ']}
        :param batchId:
        :return:
        """
        response = self._client.get(
            f"/batches/{batchId}"
        )
        return response

    def get_batches_state(self, batchId):
        response = self._client.get(
            f"/batches/{batchId}/state"
        )

        batches_id = response['id']
        state = response['state']

    def get_batches_log(self, batchId):
        response = self._client.get(
            f"/batches/{batchId}/log"
        )

        batches_id = response['id']
        offset = response['from']
        total = response['total']
        log = response['log']

        return log

    def delete_batches(self, batchId):
        self._client.delete(f"/batches/{batchId}")


def polling_intervals(
        start: Iterable[float], rest: float, max_duration: float = None
) -> Iterator[float]:
    def _intervals():
        yield from start
        while True:
            yield rest

    cumulative = 0.0
    for interval in _intervals():
        cumulative += interval
        if max_duration is not None and cumulative > max_duration:
            break
        yield interval


class BatchesState(Enum):
    WAITING = "waiting"
    RUNNING = "running"
    AVAILABLE = "available"
    ERROR = "error"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"
    SUCCESS = "success"
    DEAD = "dead"


class Batches(object):

    def __init__(
            self,
            url: str,
            auth: Auth = None,
            verify: Verify = True,
            proxy_user: str = None,
            jars: List[str] = None,
            py_files: List[str] = None,
            files: List[str] = None,
            driver_memory: str = None,
            driver_cores: int = None,
            executor_memory: str = None,
            executor_cores: int = None,
            num_executors: int = None,
            archives: List[str] = None,
            queue: str = None,
            name: str = None,
            spark_conf: Dict[str, Any] = None,
            echo: bool = True,
            check: bool = True,

            file: str = None,
            className: str = None,
            args: List[str] = None,

    ) -> None:
        self.client = LivyBatches(url, auth, verify=verify)
        self.proxy_user = proxy_user
        self.jars = jars
        self.py_files = py_files
        self.files = files
        self.driver_memory = driver_memory
        self.driver_cores = driver_cores
        self.executor_memory = executor_memory
        self.executor_cores = executor_cores
        self.num_executors = num_executors
        self.archives = archives
        self.queue = queue
        self.name = name
        self.spark_conf = spark_conf
        self.echo = echo
        self.check = check
        self.batchId: Optional[int] = None

        self.file = file
        self.className = className
        self.args = args

    def __enter__(self) -> "Batches":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def close(self) -> None:
        """Kill the managed Spark session."""
        if self.batchId is not None:
            self.client.delete_batches(self.batchId)
        self.client.close()

    def run(self,
            proxy_user: str = None,
            jars: List[str] = None,
            py_files: List[str] = None,
            files: List[str] = None,
            driver_memory: str = None,
            driver_cores: int = None,
            executor_memory: str = None,
            executor_cores: int = None,
            num_executors: int = None,
            archives: List[str] = None,
            queue: str = None,
            name: str = None,
            spark_conf: Dict[str, Any] = None,

            file: str = None,
            className: str = None,
            args: List[str] = None,
            ):
        """Run some code in the managed Spark session.

        :param code: The code to run.
        """

        # 初始化时的参数设置
        body = {
            "file": self.file,
            "className": self.className,
            "name": self.name,
        }

        if self.args is not None:
            body["args"] = self.args
        if self.proxy_user is not None:
            body["proxyUser"] = self.proxy_user
        if self.jars is not None:
            body["jars"] = self.jars
        if self.py_files is not None:
            body["pyFiles"] = self.py_files
        if self.files is not None:
            body["files"] = self.files
        if self.driver_memory is not None:
            body["driverMemory"] = self.driver_memory
        if self.driver_cores is not None:
            body["driverCores"] = self.driver_cores
        if self.executor_memory is not None:
            body["executorMemory"] = self.executor_memory
        if self.executor_cores is not None:
            body["executorCores"] = self.executor_cores
        if self.num_executors is not None:
            body["numExecutors"] = self.num_executors
        if self.archives is not None:
            body["archives"] = self.archives
        if self.queue is not None:
            body["queue"] = self.queue
        if self.spark_conf is not None:
            body["conf"] = self.spark_conf

        # 运行时的参数设置

        if name is not None:
            body["name"] = name
        if file is not None:
            body["file"] = file
        if className is not None:
            body["className"] = className

        if args is not None:
            body["args"] = args
        if proxy_user is not None:
            body["proxyUser"] = proxy_user
        if jars is not None:
            body["jars"] = jars
        if py_files is not None:
            body["pyFiles"] = py_files
        if files is not None:
            body["files"] = files
        if driver_memory is not None:
            body["driverMemory"] = driver_memory
        if driver_cores is not None:
            body["driverCores"] = driver_cores
        if executor_memory is not None:
            body["executorMemory"] = executor_memory
        if executor_cores is not None:
            body["executorCores"] = executor_cores
        if num_executors is not None:
            body["numExecutors"] = num_executors
        if archives is not None:
            body["archives"] = archives
        if queue is not None:
            body["queue"] = queue
        if spark_conf is not None:
            body["conf"] = spark_conf

        app_state, app_log = self._execute(body)

        return app_state, app_log

    def _execute(self, body):

        batchId, state = self.client.post_batches(body)

        LOGGER.info('batchId : {} '.format(batchId))
        LOGGER.info('state : {} '.format(state))

        self.batchId = batchId

        intervals = polling_intervals([1, 2, 3, 5, 8], 10)

        while state not in [BatchesState.SUCCESS.value, BatchesState.DEAD.value]:
            time.sleep(next(intervals))
            state = self.client.get_batches(self.batchId)
            LOGGER.info('state : {} '.format(state))

        if state in [BatchesState.SUCCESS.value]:
            app_state = True
        else:
            app_state = False

        app_log = self.client.get_batches_log(self.batchId)

        return app_state, app_log

    def post_batches(self,
                     proxy_user: str = None,
                     jars: List[str] = None,
                     py_files: List[str] = None,
                     files: List[str] = None,
                     driver_memory: str = None,
                     driver_cores: int = None,
                     executor_memory: str = None,
                     executor_cores: int = None,
                     num_executors: int = None,
                     archives: List[str] = None,
                     queue: str = None,
                     name: str = None,
                     spark_conf: Dict[str, Any] = None,

                     file: str = None,
                     className: str = None,
                     args: List[str] = None,
                     ):
        """Run some code in the managed Spark session.

        :param code: The code to run.
        """

        # 初始化时的参数设置
        body = {
            "file": self.file,
            "className": self.className,
            "name": self.name,
        }

        if self.args is not None:
            body["args"] = self.args
        if self.proxy_user is not None:
            body["proxyUser"] = self.proxy_user
        if self.jars is not None:
            body["jars"] = self.jars
        if self.py_files is not None:
            body["pyFiles"] = self.py_files
        if self.files is not None:
            body["files"] = self.files
        if self.driver_memory is not None:
            body["driverMemory"] = self.driver_memory
        if self.driver_cores is not None:
            body["driverCores"] = self.driver_cores
        if self.executor_memory is not None:
            body["executorMemory"] = self.executor_memory
        if self.executor_cores is not None:
            body["executorCores"] = self.executor_cores
        if self.num_executors is not None:
            body["numExecutors"] = self.num_executors
        if self.archives is not None:
            body["archives"] = self.archives
        if self.queue is not None:
            body["queue"] = self.queue
        if self.spark_conf is not None:
            body["conf"] = self.spark_conf

        # 运行时的参数设置

        if name is not None:
            body["name"] = name
        if file is not None:
            body["file"] = file
        if className is not None:
            body["className"] = className

        if args is not None:
            body["args"] = args
        if proxy_user is not None:
            body["proxyUser"] = proxy_user
        if jars is not None:
            body["jars"] = jars
        if py_files is not None:
            body["pyFiles"] = py_files
        if files is not None:
            body["files"] = files
        if driver_memory is not None:
            body["driverMemory"] = driver_memory
        if driver_cores is not None:
            body["driverCores"] = driver_cores
        if executor_memory is not None:
            body["executorMemory"] = executor_memory
        if executor_cores is not None:
            body["executorCores"] = executor_cores
        if num_executors is not None:
            body["numExecutors"] = num_executors
        if archives is not None:
            body["archives"] = archives
        if queue is not None:
            body["queue"] = queue
        if spark_conf is not None:
            body["conf"] = spark_conf

        batchId, state = self.client.post_batches(body)

        LOGGER.info('batchId : {} '.format(batchId))
        LOGGER.info('state : {} '.format(state))

        return batchId, state

    def get_batches(self, batchId):
        return self.client.get_batches_response(batchId)

    def get_batches_log(self, batchId):
        return self.client.get_batches_log(batchId)

    def delete_stop_task(self, batchId):
        return self.client.delete_batches(batchId)
