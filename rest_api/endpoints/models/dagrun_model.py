#!/usr/bin/env python
# -*- coding:utf-8 -*-

from airflow.models.dagrun import DagRun
from airflow.utils.types import DagRunType
from airflow.utils.session import NEW_SESSION, provide_session
from sqlalchemy import (Column,
                        Integer, String, Boolean,
                        DateTime, PickleType, and_, or_, tuple_, func)


class EasyAirflowDagRun(DagRun):

    @classmethod
    @provide_session
    def get_scheduled_dag_runs(cls,
                     dag_ids,
                     min_execution_date=None,
                     max_execution_date=None,
                     page_num=1,
                     page_size=10,
                     states=[],
                     sort_field="data_interval_end",
                     direction=0,
                     session=None):
        base_filter = and_(
            DagRun.dag_id.in_(dag_ids) if dag_ids else True,
            DagRun.data_interval_end >= min_execution_date if min_execution_date else True,
            DagRun.data_interval_end <= max_execution_date if max_execution_date else True,
            DagRun.state.in_(states) if states else True,
            DagRun.run_type != DagRunType.MANUAL
        )
        count = session.query(func.count(DagRun.id)).filter(base_filter).scalar()
        dag_runs = session.query(DagRun).filter(base_filter).\
            order_by(getattr(DagRun, sort_field).desc() if direction else getattr(DagRun, sort_field))\
            .offset((page_num - 1) * page_size).limit(page_size).all()
        return count, dag_runs



