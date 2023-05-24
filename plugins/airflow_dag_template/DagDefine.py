from airflow.models import ID_LEN
from sqlalchemy import Column, String, Boolean, JSON

from airflow_dag_template.sqlalchemy_util import Base, props


class DagDefineModel(Base):
    __tablename__ = "l_dag_define"
    __table_args__ = {'extend_existing': True}
    dag_id = Column(String(ID_LEN), primary_key=True)
    is_publish = Column(Boolean, default=False)
    catchup = Column(Boolean, default=False)
    schedule_interval = Column(String(100))

    default_args = Column(JSON)

    def __repr__(self):
        obj_to_dict = props(self)
        return str(obj_to_dict)

    def get_obj_dict(self):
        obj_to_dict = props(self)
        obj_to_dict.pop('metadata', None)
        obj_to_dict.pop('is_publish', None)
        obj_to_dict.pop('owner', None)
        obj_to_dict.pop('registry', None)
        return obj_to_dict

