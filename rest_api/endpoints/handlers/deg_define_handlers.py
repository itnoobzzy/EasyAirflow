#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:wanglong
import logging

from endpoints.models.dagrun_model import DagRun
from utils.state import State

logger = logging.getLogger(__name__)


class DagDefineHandlers(object):


    @staticmethod
    def get_dag_run(
            dag_ids,
            states,
            min_plan_execution_date,
            max_plan_execution_date,
            page_size,
            page_num,
            reverse,
            order_by_field,
    ):



        instance_params = {"dag_ids": dag_ids,
                           "min_execution_date": min_plan_execution_date,
                           "max_execution_date": max_plan_execution_date,
                           "page_num": page_num,
                           "page_size": page_size,
                           "sort_field": order_by_field,
                           "direction": reverse,
                           "state": states
                           }

        # # 根据next_execution_date条件进行关联查询 DagRun 和 DagRunNext 已分页
        paged_all_instance_info = DagRun.get_dag_run_with_next(**instance_params)

        count_instance_info = DagRun.get_dag_run_with_next(**instance_params, count_num=True)


        # 组装分页查询的数据
        ti_info_dict_list = []
        for instance_info in paged_all_instance_info:
            if instance_info.DagRun is None or instance_info.DagRunNext is None:
                continue
            instance_dict = instance_info.DagRun.props()
            instance_next_dict = instance_info.DagRunNext.props()

            # 组装信息
            instance_dict = {**instance_dict, **instance_next_dict}

            ti_info_dict_list.append(instance_dict)

        return ti_info_dict_list, count_instance_info

    @staticmethod
    def get_dag_run_latest(
            dag_ids,
            states,
            min_plan_execution_date,
            max_plan_execution_date,
            page_size,
            page_num,
            reverse,
            order_by_field,
    ):



        instance_params = {"dag_ids": dag_ids,
                           "min_execution_date": min_plan_execution_date,
                           "max_execution_date": max_plan_execution_date,
                           "page_num": page_num,
                           "page_size": page_size,
                           "sort_field": order_by_field,
                           "direction": reverse,
                           "state": states
                           }

        # # 根据next_execution_date条件进行关联查询 DagRun 和 DagRunNext 已分页
        paged_all_instance_info = DagRun.get_dag_run_with_next(**instance_params)

        # 组装分页查询的数据
        ti_info_dict_list = []
        for instance_info in paged_all_instance_info:
            if instance_info.DagRun is None or instance_info.DagRunNext is None:
                continue
            instance_dict = instance_info.DagRun.props()
            instance_next_dict = instance_info.DagRunNext.props()

            # 组装信息
            instance_dict = {**instance_dict, **instance_next_dict}

            ti_info_dict_list.append(instance_dict)

        return ti_info_dict_list


    @staticmethod
    def get_dag_run_summary(
            dag_ids,
            states,
            min_plan_execution_date,
            max_plan_execution_date
    ):

        if states is None:
            states = State.dag_states

        # # 对task instance进行过滤
        if min_plan_execution_date is not None and max_plan_execution_date is not None:
            # # 获取所有符合时间过滤的task_id 和 execution_date
            all_filter_task_info = DagRun.get_dag_run_filter_execution_date(
                dag_ids=dag_ids,
                min_execution_date=min_plan_execution_date,
                max_execution_date=max_plan_execution_date)

            state_num = {}
            sum_num = DagRun.get_dag_run_state_num_by_id(id_date_list=all_filter_task_info,count=True)
            state_num["count"] = sum_num

            for state in states:

                state_count = DagRun.get_dag_run_state_num_by_id( id_date_list=all_filter_task_info, state=state)
                state_num[state] = state_count

        else:
            state_num = {}
            sum_num = DagRun.get_dag_run_state_num_by_id( id_list=dag_ids,count=True)
            state_num["count"] = sum_num

            for state in states:
                state_count = DagRun.get_dag_run_state_num_by_id( id_list=dag_ids, state=state)
                state_num[state] = state_count

        success_state_count = state_num[State.SUCCESS]
        complete_rate = 0
        if sum_num != 0:
            complete_rate = success_state_count * 1.0 / sum_num * 100
            # 保留两位小数
            complete_rate = round(complete_rate, 2)
        state_num["complete_rate"] = complete_rate

        return state_num
