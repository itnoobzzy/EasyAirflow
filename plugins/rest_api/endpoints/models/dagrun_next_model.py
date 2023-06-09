#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:wanglong
from airflow.utils.session import provide_session
from sqlalchemy import (Column,
                        String, DateTime)

from rest_api.utils.airflow_database import Base, airflow_provide_session
from rest_api.utils.times import dt2ts


class DagRunNext(Base):
    """
    DagRun describes an instance of a Dag. It can be created
    by the scheduler (for regular runs) or by an external trigger
    """
    __table_args__ = {'extend_existing': True}

    __tablename__ = "dag_run_next"

    dag_id = Column(String(250), primary_key=True)
    execution_date = Column(DateTime, primary_key=True)
    next_execution_date = Column(DateTime, primary_key=True)


    def props(self):

        name_list = {
            'dag_id', 'execution_date', 'next_execution_date'

        }

        date_list = {
            'next_execution_date','execution_date'
        }

        float_list = {
        }

        pr = {}
        for name in dir(self):
            value = getattr(self, name)

            if name in float_list:
                if value is not None:
                    pr[name] = int(value * 1000)
                else:
                    # pr[name] = value
                    pr[name] = 0

            elif name in date_list:
                if value is not None:
                    # pr[name] = int(value.timestamp() * 1000)
                    pr[name] = int(dt2ts(value) * 1000)
                else:
                    # pr[name] = value
                    pr[name] = 0
            elif name in name_list:
                pr[name] = value

        # 获取对象的唯一主键
        dag_id = pr['dag_id']
        execution_date = pr['execution_date']
        unique_key = '{}-{}'.format(dag_id, execution_date)

        pr['unique_key'] =unique_key

        pr['plan_execution_date'] = pr['next_execution_date']
        return pr

    def unique_key(self):

        dict_info = self.props()

        dag_id = dict_info['dag_id']
        execution_date = dict_info['execution_date']

        unique_key = '{}-{}'.format(dag_id, execution_date)

        return unique_key

    @staticmethod
    @provide_session
    def get_dag_run_next_by_dag_id_execution_date(dag_id, execution_date, session=None):
        qry = session.query(DagRunNext)\
            .filter(DagRunNext.dag_id == dag_id) \
            .filter(DagRunNext.execution_date == execution_date)

        dagruns = qry.first()

        return dagruns

        pass

