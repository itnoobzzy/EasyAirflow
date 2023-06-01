#!/usr/bin/env python
# -*- coding:utf-8 -*-
import logging
import os

from sqlalchemy import (Column,
                        String, Boolean,
                        JSON, DateTime)

from datetime import datetime
from config import DAGS_DIR, AIRFLOW_UPLOAD_DAG_HOST, AIRFLOW_UPLOAD_DAG_PORT, AIRFLOW_DAGS_DIR
from utils.airflow_web_dag_handlers import DagHandlers, dag_template_str
from utils.database import Base, landsat_provide_session
from utils.exceptions import DagsException

logger = logging.getLogger(__name__)


class DagDefine(Base):
    __tablename__ = 'l_dag_define'

    dag_id = Column(String(250), primary_key=True, comment='dag名称, 全局唯一')
    is_publish = Column(Boolean, default=False, comment='是否发布')
    deletion = Column(Boolean, default=False, comment='删除')
    catchup = Column(Boolean, default=False, comment='是否追赶')
    schedule_interval = Column(String(1024), nullable=True, comment='调度周期，与 cron 格式一致，但是没有秒,只执行一次 @oncev')
    default_args = Column(JSON, nullable=True, comment='默认参数')
    ctime = Column(DateTime, default=datetime.now, nullable=True, comment='创建时间')
    mtime = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=True, comment='修改时间')

    def __repr__(self):
        return self.dag_id

    def __init__(self,
                 dag_id=None
                 , schedule_interval=None
                 , is_publish=False
                 , catchup=False
                 , deletion=False
                 ):
        # 初始化 dag
        self.dag_id = dag_id
        self.schedule_interval = schedule_interval
        self.is_publish = is_publish
        self.catchup = catchup
        self.deletion = deletion
        pass

    @staticmethod
    @landsat_provide_session
    def get_dag(dag_id, session=None):
        qry = session.query(DagDefine).filter(
            DagDefine.dag_id == dag_id,
        )
        dag = qry.first()
        return dag

    @staticmethod
    @landsat_provide_session
    def is_exist(dag_id, session=None):
        dag = DagDefine.get_dag(dag_id, session=session)
        if dag is not None:
            return True
        else:
            return False

    @landsat_provide_session
    def upsert(self, session=None):
        dag_id = self.dag_id
        if DagDefine.is_exist(dag_id, session=session):
            session.merge(self)
        else:
            session.add(self)
        session.commit()

    @staticmethod
    @landsat_provide_session
    def publish_dag_file(dag_id, session=None):
        """上传 dag 前需要先判断 dag 是否保存成功"""
        dag_define = session.query(DagDefine) \
            .filter(DagDefine.is_publish == True) \
            .filter(DagDefine.dag_id == dag_id) \
            .first()
        if dag_define is not None:
            dag_id = dag_define.dag_id
            DagHandlers.upload_dag_local(airflow_dag_base_dir=DAGS_DIR, dag_id=dag_id)
        else:
            raise DagsException("{} dag publish is true not exist ".format(dag_id))

    @staticmethod
    @landsat_provide_session
    def delete_dag_by_dag_id(dag_id, session=None):
        dag_define = DagDefine.get_dag(dag_id=dag_id)
        if dag_define is None:
            logger.error('dag is not exist')
            raise Exception('dag_id:{} is not exist'.format(dag_id))

        # 先删除 dag 文件
        # 对应请求 Airflow  webserver api 不能清除元数据
        # 删除超时，先不调用删除接口
        # DagHandlers.delete_dag(host=AIRFLOW_UPLOAD_DAG_HOST, port=AIRFLOW_UPLOAD_DAG_PORT, dag_id=dag_id)
        DagHandlers.delete_dag_local(airflow_dag_base_dir=AIRFLOW_DAGS_DIR, dag_id=dag_id)

        # 再注销
        dag_define.is_publish = False
        dag_define.upsert()

