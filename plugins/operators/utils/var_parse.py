#!/usr/bin/env python
# -*- coding:utf-8 -*-
import re
from datetime import datetime

from operators.utils.next_execution_date import NextExecutionDate


class VarParse():

    utc_timestamp = None

    @staticmethod
    def utc_timestamp_to_strftime(utc_timestamp, date_strf):
        return datetime.utcfromtimestamp(utc_timestamp).strftime(date_strf)


    @staticmethod
    def re_replace_datetime_var(sql_str,utc_timestamp):

        VarParse.utc_timestamp = utc_timestamp

        base_pattern = '\$\{[^\$]*,[-+]\d+\}'
        pattern = '(?P<value>{})'.format(base_pattern)
        new_sql = re.sub(pattern, VarParse.datetime_var, sql_str.strip())

        return new_sql


    @staticmethod
    def datetime_var(matched):
        value = matched.group('value')

        # 删除空格，再 ${ 和 }
        date_strf_num = value.strip()[2:-1]
        date_strf_num_list = date_strf_num.split(',')

        date_strf = date_strf_num_list[0]
        second_num = date_strf_num_list[1]

        if second_num.startswith('+'):
            # 去掉符号
            num_value = second_num[1:]

            date_str = VarParse.utc_timestamp_to_strftime(VarParse.utc_timestamp + int(num_value), date_strf)

            return date_str

        elif second_num.startswith('-'):
            # 去掉符号
            num_value = second_num[1:]

            date_str = VarParse.utc_timestamp_to_strftime(VarParse.utc_timestamp - int(num_value), date_strf)

            return date_str

    @staticmethod
    def operator_re_replace_datetime_var(sql, context):
        """
        替换 sql 中和 执行计划相关的变量
        :param execution_date_timestamp:
        :return:
        """
        """

        """
        execution_date = context['execution_date']

        next_execution_date = NextExecutionDate.get_task_instance_next_execution(context)
        print("get_task_instance_next_execution next_execution_date {}".format(next_execution_date))
        if next_execution_date is None:
            next_execution_date = context['dag'].following_schedule(execution_date)


        next_next_execution_date = context['dag'].following_schedule(next_execution_date)
        print(
            'Checking  execution_date: {} next_execution_date: {} next_next_execution_date: {}'.format(
                execution_date, next_execution_date, next_next_execution_date
            )
        )

        # 默认都先去计划调度时间,后面只需要 next_execution_date 稳定后，注释掉就可以了
        # next_execution_date = execution_date

        execution_date_timestamp = next_execution_date.timestamp()
        # 默认为 0 时区，需要给用户加上8小时
        execution_date_timestamp = execution_date_timestamp + 8*60*60


        complete_sql = VarParse.re_replace_datetime_var(sql, execution_date_timestamp)

        return complete_sql


    @staticmethod
    def split_sql(str_sql):

        str_sql =  str_sql.strip().rstrip(';')

        split_sql_list = re.split(r';[\n\r]', str_sql)

        return split_sql_list

    pass

