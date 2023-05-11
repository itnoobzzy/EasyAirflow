
from airflow.models import ID_LEN
from sqlalchemy import Column, String, Integer

from airflow_dag_template.sqlalchemy_util import Base, props, provide_session


class DagTaskDepModel(Base):

    # __tablename__ = "dag_task_dep_3"
    __tablename__ = "l_dag_task_dep"
    __table_args__ = {'extend_existing': True}

    dag_id = Column(String(ID_LEN), primary_key=True)
    task_id = Column(String(ID_LEN), primary_key=True)
    upstream_task_id = Column(String(ID_LEN), primary_key=True)

    # todo 当获取血缘关系时，选取所有1的，当生产 dag 依赖时，去除在 1 中，存在与 2 中 上游依赖模式匹配的
    type = Column(Integer,nullable=True,comment='依赖类型：1 用户添加的依赖，2  dag 维护外部依赖添加')


    # Lets us print out a user object conveniently.
    def __repr__(self):

        obj_to_dict = props(self)

        return str(obj_to_dict)


    def get_obj_dict(self):

        obj_to_dict = props(self)

        return obj_to_dict

@provide_session
def sqlalchemy_add(session=None):
    # Let's create a user and add two e-mail addresses to that user.

    ed_task = DagTaskDepModel(

        task_id='t2',
        upstream_task_id='t1',
        dag_id = 'dag_1',

    )

    ed_task = DagTaskDepModel(

        task_id='t3',
        upstream_task_id='t2',
        dag_id = 'dag_1',

    )

    # Let's add the user and its addresses we've created to the DB and commit.
    session.add(ed_task)
    session.commit()


# if __name__ == '__main__':
#
#     # Create all tables by issuing CREATE TABLE commands to the DB.
#     Base.metadata.create_all(engine)
#
#     # pip install mysqlclient
#     sqlalchemy_add()
#
#
#     res_value_list = session.query(DagTaskDepModel) \
#         .filter(DagTaskDepModel.dag_id == 'dag_1') \
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
#         # config = dagDefineModel_to_dagDefine(res_value)
#         #
#         # print(str(config))