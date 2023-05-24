
from airflow.models import ID_LEN
from sqlalchemy import Column, Integer, String, DateTime, Boolean, JSON

from airflow_dag_template.sqlalchemy_util import Base, props


class TaskDefineModel(Base):

    __tablename__ = "l_task_define"
    __table_args__ = {'extend_existing': True}

    task_id = Column(String(ID_LEN), primary_key=True)
    dag_id = Column(String(ID_LEN), primary_key=True)
    operator = Column(String(100))

    owner = Column(String(100))
    email = Column(String(500))
    email_on_retry = Column(Boolean)
    email_on_failure = Column(Boolean)

    start_date = Column(DateTime)
    end_date = Column(DateTime)

    trigger_rule = Column(String(50), default='all_success')
    depends_on_past = Column(Boolean, default=False)
    wait_for_downstream = Column(Boolean, default=False)
    schedule_interval = Column(String(100))

    retries = Column('retries', Integer, default=0)
    retry_delay_num_minutes = Column(Integer)
    execution_timeout_num_minutes = Column(Integer)

    pool = Column(String(50))
    queue = Column(String(256))
    priority_weight = Column(Integer)

    private_params = Column(JSON)
    is_publish = Column(Boolean, default=False, comment='是否发布')

    def __repr__(self):
        obj_to_dict = props(self)
        return str(obj_to_dict)



