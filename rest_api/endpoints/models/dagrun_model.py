#!/usr/bin/env python
# -*- coding:utf-8 -*-
import datetime

from airflow.models.dagrun import DagRun
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from airflow.utils.session import NEW_SESSION, provide_session
from sqlalchemy import and_, func
from sqlalchemy.orm import Session

import config


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
                     session: Session = NEW_SESSION):
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

    @classmethod
    @provide_session
    def trigger_dags(cls,
                     dag_ids,
                     execution_dates,
                     session: Session = NEW_SESSION):
        """
        批量触发 dag
        :param dag_ids:
        :param execution_dates:
        :param session:
        :return:
        """
        drs = session.query(DagRun).filter(
            DagRun.dag_id.in_(dag_ids),
            DagRun.execution_date.in_(execution_dates),
        ).all()

        for dr in drs:
            dr.state = State.RUNNING
            dr.start_date = datetime.datetime.now(tz=config.TIMEZONE)
            session.merge(dr)
        session.commit()
