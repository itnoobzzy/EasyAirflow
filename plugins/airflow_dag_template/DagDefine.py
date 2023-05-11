from airflow.models import ID_LEN
from sqlalchemy import Column, String, Boolean, JSON

from airflow_dag_template.sqlalchemy_util import Base, props, provide_session


class DagDefineModel(Base):

    # __tablename__ = "dag_define_3"
    __tablename__ = "l_dag_define"
    __table_args__ = {'extend_existing': True}

    dag_id = Column(String(ID_LEN), primary_key=True)

    # owner = Column(String(100))

    is_publish = Column(Boolean, default=False)
    catchup = Column(Boolean, default=False)
    schedule_interval = Column(String(100))

    default_args = Column(JSON)


    # Lets us print out a user object conveniently.
    def __repr__(self):

        obj_to_dict = props(self)

        return str(obj_to_dict)

    def get_obj_dict(self):

        obj_to_dict = props(self)

        obj_to_dict.pop('metadata', None)
        obj_to_dict.pop('is_publish',None)
        obj_to_dict.pop('owner',None)

        return obj_to_dict

@provide_session
def sqlalchemy_add(session=None):
    # Let's create a user and add two e-mail addresses to that user.

    ed_task = DagDefineModel(

        dag_id = 'dag_1',

        owner = 'wanglong',

        is_publish = False,
        catchup = False,
        schedule_interval = '@once',

        default_args = {
            'owner': 'Airflow',
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
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
#
#     res_value_list = session.query(DagDefineModel) \
#         .filter(DagDefineModel.dag_id == 'dag-t1') \
#         # .filter(DagDefineModel.dag_id == 'dag_1') \
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
#         config = dagDefineModel_to_dagDefine(res_value)
#
#         print(str(config))