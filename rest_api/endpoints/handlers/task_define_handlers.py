#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:wanglong
import logging

from endpoints.handlers.airflow_task_handlers import TaskHandlers
from endpoints.models.dag_task_dep_model import DagTaskDependence
from endpoints.models.task_instance_model import TaskInstance
from endpoints.models.task_model import TaskDefine
from endpoints.models.taskinstance_next_model import TaskInstanceNext
from endpoints.models.taskinstance_type_model import TaskInstanceType
from utils.state import State
from utils.times import dt2ts

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
            task_ids,
            states,
            min_plan_execution_date,
            max_plan_execution_date,
            page_size,
            page_num,
            reverse,
            order_by_field,
            instance_type,
    ):


        # 获取补数据的 实例信息
        ti_type_info_dict_list = TaskDefineHandlers.get_ti_type_info_dict(task_ids,
                                                                          min_execution_date=None,
                                                                          max_execution_date=None)
        # # 所有补数据(task_id, execution)信息
        ti_type_tuple_list = [(i.get("task_id"), i.get("execution_date"))
                              for _, i in ti_type_info_dict_list.items()]
        cycle_type = TaskInstanceType.CYCLE_TYPE
        filldata_type = TaskInstanceType.FILL_DATA_TYPE

        type_params = {"instance_type": instance_type,
                       "cycle_type": cycle_type,
                       "filldata_type": filldata_type,
                       "ti_type_tuple_list": ti_type_tuple_list}


        instance_params = {"task_ids": task_ids,
                           "min_execution_date": min_plan_execution_date,
                           "max_execution_date": max_plan_execution_date,
                           "page_num": page_num,
                           "page_size": page_size,
                           "sort_field": order_by_field,
                           "direction": reverse,
                           "state": states
                           }

        # # 根据next_execution_date条件进行关联查询 TaskInstance和TaskInstanceNext 已分页
        paged_all_instance_info = TaskInstance.get_task_instance_with_next(**type_params,
                                                                           **instance_params)
        count_instance_info = TaskInstance.get_task_instance_with_next(**type_params,
                                                                       **instance_params,
                                                                       count_num=True)


        # 组装分页查询的数据
        ti_info_dict_list = []
        for instance_info in paged_all_instance_info:
            if instance_info.TaskInstance is None or instance_info.TaskInstanceNext is None:
                continue
            task_instance_dict = instance_info.TaskInstance.props()
            task_instance_next_dict = instance_info.TaskInstanceNext.props()

            # 组装信息
            task_instance_dict = {**task_instance_dict, **task_instance_next_dict}

            ti_info_dict_list.append(task_instance_dict)

        return ti_info_dict_list, count_instance_info


    @staticmethod
    def get_task_instance_summary(
            task_ids,
            states,
            min_plan_execution_date,
            max_plan_execution_date,
            instance_type,
    ):

        # 获取补数据的 实例信息
        ti_type_info_dict_list = TaskDefineHandlers.get_ti_type_info_dict(task_ids,
                                                                          min_execution_date=None,
                                                                          max_execution_date=None)
        # # 所有补数据(task_id, execution)信息
        ti_type_tuple_list = [(i.get("task_id"), i.get("execution_date"))
                              for _, i in ti_type_info_dict_list.items()]
        cycle_type = TaskInstanceType.CYCLE_TYPE
        filldata_type = TaskInstanceType.FILL_DATA_TYPE

        if states is None:
            states = State.task_states

        type_params = {"instance_type": instance_type,
                       "cycle_type": cycle_type,
                       "filldata_type": filldata_type,
                       "ti_type_tuple_list": ti_type_tuple_list}


        # # 对task instance进行过滤
        if min_plan_execution_date is not None and max_plan_execution_date is not None:
            # # 获取所有符合时间过滤的task_id 和 execution_date
            all_filter_task_info = TaskInstanceNext.get_instance_filter_exectuion_date(
                airflow_task_ids=task_ids,
                min_execution_date=min_plan_execution_date,
                max_execution_date=max_plan_execution_date)

            state_num = {}
            sum_num = TaskInstance.get_instance_state_num_by_id(**type_params,
                                                                id_date_list=all_filter_task_info,
                                                                count=True)
            state_num["count"] = sum_num

            for state in states:

                state_count = TaskInstance.get_instance_state_num_by_id(**type_params,
                                                                            id_date_list=all_filter_task_info,
                                                                            state=state)
                state_num[state] = state_count

        else:
            state_num = {}
            sum_num = TaskInstance.get_instance_state_num_by_id(**type_params,
                                                                id_list=task_ids,
                                                                count=True)
            state_num["count"] = sum_num

            for state in states:


                state_count = TaskInstance.get_instance_state_num_by_id(**type_params,
                                                                           id_list=task_ids,
                                                                           state=state)
                state_num[state] = state_count

        success_state_count = state_num[State.SUCCESS]
        complete_rate = 0
        if sum_num != 0:
            complete_rate = success_state_count * 1.0 / sum_num * 100
            # 保留两位小数
            complete_rate = round(complete_rate, 2)
        state_num["complete_rate"] = complete_rate

        return state_num

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
