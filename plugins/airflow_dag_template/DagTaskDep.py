from airflow.models import ID_LEN
from sqlalchemy import Column, String, Integer

from airflow_dag_template.sqlalchemy_util import Base, props


class DagTaskDepModel(Base):
    __tablename__ = "l_dag_task_dep"
    __table_args__ = {'extend_existing': True}
    dag_id = Column(String(ID_LEN), primary_key=True)
    task_id = Column(String(ID_LEN), primary_key=True)
    upstream_task_id = Column(String(ID_LEN), primary_key=True)

    # todo 当获取血缘关系时，选取所有1的，当生产 dag 依赖时，去除在 1 中，存在与 2 中 上游依赖模式匹配的
    type = Column(Integer, nullable=True, comment='依赖类型：1 用户添加的依赖，2  dag 维护外部依赖添加')

    def __repr__(self):
        obj_to_dict = props(self)
        return str(obj_to_dict)

    def get_obj_dict(self):
        obj_to_dict = props(self)
        return obj_to_dict
