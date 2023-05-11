
from airflow.models import ID_LEN

from datetime import datetime, timedelta
from sqlalchemy import Column, Integer, String, ForeignKey, create_engine, Index, DateTime, Boolean, JSON

from airflow_dag_template.sqlalchemy_util import Base, props, provide_session


class TaskDefineModel(Base):

    # __tablename__ = "task_define_3"
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
    # retry_delay = timedelta(minutes=retry_delay_num_minutes)
    retry_delay_num_minutes = Column(Integer)
    execution_timeout_num_minutes = Column(Integer)

    pool = Column(String(50))
    queue = Column(String(256))
    priority_weight = Column(Integer)

    private_params = Column(JSON)
    is_publish = Column(Boolean, default=False, comment='是否发布')


    # Lets us print out a user object conveniently.
    def __repr__(self):

        obj_to_dict = props(self)

        return str(obj_to_dict)



@provide_session
def sqlalchemy_add(session=None):
    # Let's create a user and add two e-mail addresses to that user.

    ed_task = TaskDefineModel(

        task_id='t1',
        dag_id = 'dag_1',
        operator = 'ExternalTaskSensor',

        owner = 'wanglong',
        email = 'email',
        email_on_retry = True,
        email_on_failure = True,

        start_date = datetime(2018, 11, 1),
        end_date = datetime(2019, 11, 1),

        trigger_rule = 'all_success',
        depends_on_past = False,
        wait_for_downstream = False,
        schedule_interval = '@once',
        retries = 2,
        # retry_delay = timedelta(minutes=retry_delay_num_minutes)
        retry_delay_num_minutes = 3,

        pool='default_pool',
        queue='default',
        priority_weight=1,

        private_params = {
                'external_dag_id': 'wanglong-tutorial-0114',
                'external_task_id': 'templated',
                'execution_delta_num_minutes': 1
            }
    )


    ed_task = TaskDefineModel(

        task_id='t2',
        dag_id = 'dag_1',
        operator = 'DummyOperator',

        owner = 'wanglong',
        email = 'email',
        email_on_retry = True,
        email_on_failure = True,

        start_date = datetime(2018, 11, 1),
        end_date = datetime(2019, 11, 1),

        trigger_rule = 'all_success',
        depends_on_past = False,
        wait_for_downstream = False,
        schedule_interval = '@once',
        retries = 2,
        # retry_delay = timedelta(minutes=retry_delay_num_minutes)
        retry_delay_num_minutes = 3,

        pool='default_pool',
        queue='default',
        priority_weight=1,

        private_params = {
                'external_dag_id': 'wanglong-tutorial-0114',
                'external_task_id': 'templated',
                'execution_delta_num_minutes': 1
            }
    )

    ed_task = TaskDefineModel(

        task_id='t3',
        dag_id = 'dag_1',
        operator = 'BashOperator',

        owner = 'wanglong',
        email = 'email',
        email_on_retry = True,
        email_on_failure = True,

        start_date = datetime(2018, 11, 1),
        end_date = datetime(2019, 11, 1),

        trigger_rule = 'all_success',
        depends_on_past = False,
        wait_for_downstream = False,
        schedule_interval = '@once',
        retries = 2,
        # retry_delay = timedelta(minutes=retry_delay_num_minutes)
        retry_delay_num_minutes = 3,

        pool='default_pool',
        queue='default',
        priority_weight=1,

        private_params = {
                'bash_command': 'echo "hello world xxxxxx"'
            }
    )

    # Let's add the user and its addresses we've created to the DB and commit.
    session.add(ed_task)
    session.commit()




#
# if __name__ == '__main__':
#
#     # Create all tables by issuing CREATE TABLE commands to the DB.
#     # Base.metadata.create_all(engine)
#
#     # pip install mysqlclient
#     # sqlalchemy_add()
#
#     res_value_list = session.query(TaskDefineModel) \
#         .filter(TaskDefineModel.dag_id == 'dag-t1') \
#         # .filter(TaskDefineModel.dag_id == 'dag_1') \
#         # .filter(TaskDefineModel.task_id == 't1') \
#     # .first()
#
#     print('=============================')
#
#     for res_value in res_value_list:
#         print(res_value)
#
#         print('xxxxxx')
#
#         config = taskDefineModel_to_taskDefine(res_value)
#
#         print(str(config))
#         # print(res_value.__dict__)
