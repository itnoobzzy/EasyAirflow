#!/usr/bin/env python
# -*- coding:utf-8 -*-
import logging
from datetime import datetime

import config
from rest_api.endpoints.handlers.airflow_task_handlers import TaskHandlers
from rest_api.endpoints.models.dag_task_dep_model import DagTaskDependence
from rest_api.endpoints.models.task_instance_model import EasyAirflowTaskInstance
from rest_api.endpoints.models.task_model import TaskDefine
from rest_api.endpoints.models.taskinstance_type_model import TaskInstanceType
from rest_api.utils.state import State

logger = logging.getLogger(__name__)


class TaskDefineHandlers(object):


    @staticmethod
    def get_task_instance_try_log(dag_id, task_id, execution_date, try_num):
        # url = get_task_instance_try_log(task.dag_id, task_id, ti.execution_date, try_num)
        url,max_try_num = TaskHandlers.get_task_log_url(dag_id, task_id, execution_date, try_num)
        if try_num is None:
            try_num = max_try_num

        log_context = "*** Fetching from: {}\n".format(url)
        try:
            import requests
            timeout = 1000
            response = requests.get(url, timeout=timeout)
            response.encoding = "utf-8"

            # Check if the resource was properly fetched
            response.raise_for_status()

            log_context += '\n' + response.text
        except Exception as e:
            log_context += "*** Failed to fetch log file from worker. {}\n".format(str(e))

        # task = Task.get_task(task_id)
        item = {

            "task_id": task_id
            # , "execution_date": int(dt2ts(execution_date) * 1000)
            , "try_num": try_num
            , "max_try_num": max_try_num
            , "log_url": url
            , "log_context": log_context
        }

        return item


    @staticmethod
    def get_ti_type_info_dict(task_ids, min_execution_date, max_execution_date):
        # 拼接查询条件
        TI = TaskInstanceType
        param = []
        if len(task_ids) >= 0:
            param.append(TI.task_id.in_(task_ids))

        if min_execution_date is not None and max_execution_date is not None:
            param.append(TI.execution_date.between(min_execution_date, max_execution_date))

        ti_types = TaskInstanceType.get_task_all_instance(param)

        ti_type_info_dict_list = {}
        for ti_type in ti_types:

            dict_key = ti_type.unique_key()

            ti_type_info_dict = ti_type.props()

            ti_type_info_dict_list[dict_key] = ti_type_info_dict

        return ti_type_info_dict_list

    @staticmethod
    def get_task_instance(
            dag_id,
            task_ids,
            states,
            min_plan_execution_date,
            max_plan_execution_date,
            page_size,
            page_num,
            reverse,
            order_by_field
    ):
        query_params = {"dag_id": dag_id}
        if min_plan_execution_date:
            min_plan_execution_date = datetime.fromtimestamp(min_plan_execution_date / 1000, tz=config.TIMEZONE)
            query_params["min_execution_date"] = min_plan_execution_date
        if max_plan_execution_date:
            max_plan_execution_date = datetime.fromtimestamp(max_plan_execution_date / 1000, tz=config.TIMEZONE)
            query_params["max_execution_date"] = max_plan_execution_date
        if order_by_field and reverse:
            query_params["sort_field"] = order_by_field
            query_params["direction"] = reverse
        if not states:
            query_params["states"] = State.task_states
        if task_ids:
            query_params["task_ids"] = task_ids
        if page_num and page_size:
            query_params["page_num"] = page_num
            query_params["page_size"] = page_size

        count, ti_list = EasyAirflowTaskInstance.get_ti_list(**query_params)
        list_infos = [{
            "dag_id": i[0].dag_id,
            "task_id": i[0].task_id,
            "run_id": i[0].run_id,
            "data_interval_start": int(i[1].data_interval_start.timestamp() * 1000),
            "data_interval_end": int(i[1].data_interval_end.timestamp() * 1000),
            "start_date": int(i[0].start_date.timestamp() * 1000) if i[0].start_date else None,
            "end_date": int(i[0].end_date.timestamp() * 1000) if i[0].end_date else None,
            "duration": i[0].duration,
            "state": i[0].state,
            "try_number": i[0].try_number,
            "execution_date": int(i[1].execution_date.timestamp() * 1000),
            "external_trigger": i[1].external_trigger,
            "run_type": i[1].run_type,
            "pool": i[0].pool,
            "queue": i[0].queue,
            "priority_weight": i[0].priority_weight
        } for i in ti_list]
        return count, list_infos

    @staticmethod
    def get_task_instance_summary(
            dag_id,
            task_ids,
            states,
            min_plan_execution_date,
            max_plan_execution_date
    ):
        if min_plan_execution_date:
            min_plan_execution_date = datetime.fromtimestamp(min_plan_execution_date / 1000, tz=config.TIMEZONE)
        if max_plan_execution_date:
            max_plan_execution_date = datetime.fromtimestamp(max_plan_execution_date / 1000, tz=config.TIMEZONE)
        if not states:
            states = State.task_states

        state_num = EasyAirflowTaskInstance.get_ti_states_summary(
            dag_id,
            task_ids=task_ids,
            states=states,
            min_data_interval_end=min_plan_execution_date,
            max_data_interval_end=max_plan_execution_date
        )
        return {state: dict(state_num).get(state, 0) for state in State.task_states}

    @staticmethod
    def get_external_task_id(task_id):
        """
        获取指定任务的影子任务（额外任务）
        :param task_id:
        :return: 返回额外任务的 id
        """

        upstream_up_ids = DagTaskDependence.external_get_task_up_depends(task_id)

        external_task_ids = []

        for upstream_up_id in upstream_up_ids:
            if TaskDefine.is_external_task_id(task_id, upstream_up_id):
                external_task_ids.append(upstream_up_id)

        return external_task_ids
