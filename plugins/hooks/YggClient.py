from enum import Enum
import logging
from typing import Iterable, Iterator
from typing import Union, Tuple

import requests
from requests import HTTPError
from requests.auth import HTTPBasicAuth

Auth = Union[requests.auth.AuthBase, Tuple[str, str]]
Verify = Union[bool, str]

LOGGER = logging.getLogger(__name__)


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


class JsonClient:
    """A wrapper for a requests session for JSON formatted requests.

    This client handles appending endpoints on to a common hostname,
    deserialising the response as JSON and raising an exception when an error
    HTTP code is received.
    """

    def __init__(
            self, url: str, auth: Auth = None, verify: Verify = True, headers=None
    ) -> None:
        self.url = url
        self.session = requests.Session()
        if auth is not None:
            self.session.auth = auth
        self.session.verify = verify
        if headers:
            self.session.headers.update(headers)

    def close(self) -> None:
        self.session.close()

    def get(self, endpoint: str = "") -> dict:
        return self._request("GET", endpoint)

    def post(self, endpoint: str, data: dict = None) -> dict:
        return self._request("POST", endpoint, data)

    def put(self, endpoint: str, data: dict = None) -> dict:
        return self._request("PUT", endpoint, data)

    def delete(self, endpoint: str = "") -> dict:
        return self._request("DELETE", endpoint)

    def my_raise_for_status(self, response):

        reason = response.reason
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
        pass

    def _request(self, method: str, endpoint: str, data: dict = None) -> dict:
        url = self.url.rstrip("/") + endpoint
        response = self.session.request(method, url, json=data)
        # 不使用自带的，不抛出具体的异常
        # response.raise_for_status()
        self.my_raise_for_status(response)
        return response.json()


class YggClient(object):

    def __init__(
            self, url: str, auth: Auth = None, verify: Verify = True, headers=None
    ) -> None:
        self._client = JsonClient(url, auth, verify, headers)

    def close(self) -> None:
        """Close the underlying requests session."""
        self._client.close()

    def post_task(self, data, app_type):
        response = self._client.post(
            f"/{app_type}/v1/scheduler/task", data=data
        )

        LOGGER.info('post_batches response : {} '.format(response))

        return response

    def post_task_not_run(self, data, app_type):
        response = self._client.post(
            f"/{app_type}/v1/scheduler/task/create", data=data
        )
        LOGGER.info('post_batches response : {} '.format(response))
        return response

    def get_task_status(self, task_id, app_type):
        response = self._client.get(
            f"/{app_type}/v1/scheduler/task/status/{task_id}"
        )
        return response

    def get_task_submit_log(self, task_id, app_type):
        response = self._client.get(
            f"/{app_type}/v1/scheduler/task/submit/log/{task_id}"
        )
        return response

    def get_task_execution_log(self, task_id, app_type):
        response = self._client.get(
            f"/{app_type}/v1/scheduler/task/execution/log/{task_id}"
        )

        return response

    def delete_task(self, task_id, app_type):
        self._client.delete(f"/{app_type}/v1/scheduler/task/{task_id}")

    def put_task_invoke(self, task_id, app_type, data):
        response = self._client.put(f"/{app_type}/v1/scheduler/task/invoke/{task_id}"
                                    , data=data
                                    )

        return response


class YggState(Enum):
    """
    READY(10, "准备"),
    PREPARING(20, ""),
    RUNNING(30, "正在运行"),
    PAUSED(40, "暂停"),
    SUCCEEDED(50, "运行成功"),
    KILLING(55, "正在杀死"),
    KILLED(60, "已杀死"),
    FAILED(70, "运行失败"),
    FAILED_FINISHING(80, "运行失败，完成状态"),
    SKIPPED(90, "跳过运行"),
    DISABLED(100, "暂停运行"),
    QUEUED(110, "队列中"),
    FAILED_SUCCEEDED(120, "运行成功，完成装填"),
    CANCELLED(125, "取消"),
    WAIT_RESOURCE(130, "等待资源"),

    ACCEPTED(135, "接受任务"),
    FINISHED(140, "运行结束"),
    UNKNOWN(145, "未知状态");
    """
    READY = 10
    PREPARING = 20
    RUNNING = 30
    PAUSED = 40
    SUCCEEDED = 50
    KILLING = 55
    KILLED = 60
    FAILED = 70
    FAILED_FINISHING = 80
    SKIPPED = 90
    DISABLED = 100
    QUEUED = 110
    FAILED_SUCCEEDED = 120
    CANCELLED = 125
    WAIT_RESOURCE = 130

    ACCEPTED = 135
    FINISHED = 140
    UNKNOWN = 145


class YggServer(object):

    def __init__(
            self,
            url: str,
            auth: Auth = None,
            headers=None,
            verify: Verify = True,
    ) -> None:
        self.client = YggClient(url, auth, verify=verify, headers=headers)

    def __enter__(self) -> "YggServer":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def close(self) -> None:
        """Kill the managed Spark session."""
        # if self.batchId is not None:
        #     self.client.delete_task(self.batchId)
        self.client.close()

    def post_batches(self, app_type
                     , engineParams
                     , taskParams
                     , environment
                     , task_name
                     , task_desc
                     , user_id):
        # 初始化时的参数设置
        body = {
            "engineParams": engineParams,
            "taskParams": taskParams,
            "envTypeId": environment,
            "taskName": task_name,
            "taskDesc": task_desc,
            "userId": user_id,
        }

        response = self.client.post_task(body, app_type=app_type)

        LOGGER.info('batchId : {} '.format(response))

        taskId = response['data']['taskId']
        return taskId

    def create_task(self, data, app_type):
        response = self.client.post_task(data=data, app_type=app_type)

        LOGGER.info('batchId : {} '.format(response))

        taskId = response['data']['taskId']
        return taskId

    def create_task_not_run(self, data, app_type):
        response = self.client.post_task_not_run(data=data, app_type=app_type)

        LOGGER.info('batchId : {} '.format(response))

        taskId = response['data']['taskId']
        return taskId

    def get_task_status(self, task_id, app_type):
        response = self.client.get_task_status(task_id=task_id, app_type=app_type)

        return response

    def get_task_submit_log(self, task_id, app_type):
        response = self.client.get_task_submit_log(task_id=task_id, app_type=app_type)

        return response['data']

    def get_task_execution_log(self, task_id, app_type):
        response = self.client.get_task_execution_log(task_id=task_id, app_type=app_type)

        return response['data']

    def delete_stop_task(self, task_id, app_type):
        self.client.delete_task(task_id=task_id, app_type=app_type)

    def put_task_invoke(self, task_id, app_type, data):
        response = self.client.put_task_invoke(task_id=task_id, app_type=app_type, data=data)
        LOGGER.info('batchId : {} '.format(response))
        return response
