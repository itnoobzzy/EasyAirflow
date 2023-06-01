#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:wanglong
from datetime import datetime

from sqlalchemy import (Column,
                        String, Integer, DateTime)

from utils.airflow_dep_graph import Graph, GraphTraverse
from utils.database import Base, landsat_provide_session


class DagTaskDependence(Base):
    __tablename__ = 'l_dag_task_dep'

    dag_id = Column(String(200), nullable=True, comment='dag 名称，全局唯一')
    task_id = Column(String(200), primary_key=True, comment='task 名称，在同一个 dag 中只能出现一个')
    upstream_task_id = Column(String(200), primary_key=True, comment='上游任务依赖')
    # 需要增加一个 是否为外部依赖的标识，用于在血缘分析时，不出现外部依赖的任务，外部依赖任务可以和下游任务当作一个整体
    # todo 当获取血缘关系时，选取所有1的，当生产 dag 依赖时，去除在 1 中，存在与 2 中 上游依赖模式匹配的
    type = Column(Integer, primary_key=True, comment='依赖类型：1 用户添加的依赖，2  dag 维护外部依赖添加')
    ctime = Column(DateTime, default=datetime.now, nullable=True, comment='创建时间')
    mtime = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=True, comment='修改时间')
    trigger_rule = Column(Integer, nullable=True, comment='1 成功执行, 2 失败执行, 3 都执行')

    USER_DEP_TYPE = 1
    AIRFLOW_DEP_TYPE = 2

    def __init__(self, dag_id, task_id, upstream_task_id, type=2, trigger_rule=0):
        self.dag_id = dag_id
        self.task_id = task_id
        self.upstream_task_id = upstream_task_id
        self.type = type
        self.trigger_rule = trigger_rule

    @landsat_provide_session
    def upsert(self, session=None):
        task_id = self.task_id
        upstream_task_id = self.upstream_task_id
        if DagTaskDependence.is_exist(task_id,upstream_task_id):
            session.merge(self)
        else:
            session.add(self)

        session.commit()

    @staticmethod
    @landsat_provide_session
    def is_exist(task_id, upstream_task_id, session=None):
        task = DagTaskDependence.get_task_depends(task_id, upstream_task_id)

        if task is not None:
            return True
        else:
            return False

    @landsat_provide_session
    def session_add(self, session=None):
        session.add(self)

    @classmethod
    @landsat_provide_session
    def get_all_downstream(cls, upstream_task_id, session=None):
        all_task_dep = session.query(DagTaskDependence). \
            filter(DagTaskDependence.upstream_task_id == upstream_task_id).all()
        task_id_list = [i.task_id for i in all_task_dep]
        return task_id_list

    @classmethod
    @landsat_provide_session
    def get_all_downstream_recursive(cls, dag_id,upstream_task_id, session=None):

        all_task_dep = session.query(DagTaskDependence). \
            filter(DagTaskDependence.dag_id == dag_id).all()


        graph_t = GraphTraverse()

        for task_dep in all_task_dep:
            up_key = task_dep.upstream_task_id
            down_value = task_dep.task_id
            graph_t.addEdge(up_key, down_value)

        view_nodes = graph_t.graph_traverse_distinct(upstream_task_id)

        return view_nodes

    @staticmethod
    @landsat_provide_session
    def get_task_depends(task_id, upstream_task_id, session=None):
        TD = DagTaskDependence

        qry = session.query(DagTaskDependence).filter(
            # Task.dag_id == dag_id,
            TD.task_id == task_id,
            TD.upstream_task_id == upstream_task_id,

        )
        task = qry.first()

        return task

    @staticmethod
    @landsat_provide_session
    def get_task_depends_by_type(task_id, upstream_task_id,type=USER_DEP_TYPE, session=None):
        TD = DagTaskDependence

        qry = session.query(DagTaskDependence).filter(
            # Task.dag_id == dag_id,
            TD.task_id == task_id,
            TD.upstream_task_id == upstream_task_id,
            TD.type == type

        )
        task = qry.first()

        return task

    @staticmethod
    @landsat_provide_session
    def delete_dep_by_task_id(task_id, session=None):
        """
        根据 task id 删除所有的依赖
        :param task_id:
        :param session:
        :return:
        """
        TD = DagTaskDependence

        qry = session.query(DagTaskDependence).filter(
            # Task.dag_id == dag_id,
            TD.task_id == task_id

        )
        tds = qry.all()

        for td in tds:
            session.delete(td)

    @staticmethod
    @landsat_provide_session
    def delete_dep_by_upstream_task_id(upstream_task_id, session=None):
        """
        根据 upstream_task_id 删除所有的依赖
        :param upstream_task_id:
        :param session:
        :return:
        """
        TD = DagTaskDependence

        qry = session.query(DagTaskDependence).filter(
            # Task.dag_id == dag_id,
            TD.upstream_task_id == upstream_task_id

        )
        tds = qry.all()

        for td in tds:
            session.delete(td)

    @staticmethod
    @landsat_provide_session
    def get_task_down_depends(upstream_task_id, session=None):

        type = DagTaskDependence.USER_DEP_TYPE

        TD = DagTaskDependence

        qry = session.query(TD).filter(
            # Task.dag_id == dag_id,
            TD.upstream_task_id == upstream_task_id,
            TD.type == type

        )

        tds = qry.all()

        task_ids = []
        for td in tds:
            task_ids.append(td.task_id)

        return task_ids

    @staticmethod
    @landsat_provide_session
    def get_task_up_depends(task_id, session=None):

        type = DagTaskDependence.USER_DEP_TYPE
        TD = DagTaskDependence

        qry = session.query(TD).filter(
            # Task.dag_id == dag_id,
            TD.task_id == task_id,
            TD.type == type

        )

        tds = qry.all()

        task_ids = []
        for td in tds:
            task_ids.append(td.upstream_task_id)

        return task_ids

    @staticmethod
    @landsat_provide_session
    def has_child(id, session=None):
        task_ids = DagTaskDependence.get_task_down_depends(id)

        if len(task_ids) == 0:
            return False
        else:
            return True

    @staticmethod
    @landsat_provide_session
    def recursion_get_task_down_depends_depth(id, child, depth, session=None):

        if depth == 0:
            return
        depth -= 1

        task_ids = DagTaskDependence.get_task_down_depends(id)

        for task_id in task_ids:

            if DagTaskDependence.has_child(task_id):

                print(id, '-', task_id)

                # child.append(task_id)

                new_child = []
                node = {
                    'id': task_id,
                    'child': new_child
                }

                child.append(node)

                DagTaskDependence.recursion_get_task_down_depends_depth(task_id, new_child, depth)



            else:
                print(id, '-', task_id)

                # child.append(task_id)

                node = {'id': task_id}

                child.append(node)

    # todo ui 用户添加依赖
    @classmethod
    @landsat_provide_session
    def user_add_task_dep(cls, task_id, upstream_task_id, trigger_rule=1, session=None):
        """
        配置任务依赖
        :param session: 数据库连接
        :param task_id: 任务id
        :param upstream_task_id: 依赖的任务id
        :param type: 用户添加类型 USER_DEP_TYPE,为默认值
        :return:
        """

        type = cls.USER_DEP_TYPE

        TD = DagTaskDependence

        qry = session.query(DagTaskDependence).filter(
            # Task.dag_id == dag_id,
            TD.type == type,
            TD.task_id == task_id,
            TD.upstream_task_id == upstream_task_id,

        )

        task_dep = qry.first()

        if task_dep is not None:
            raise Exception('Exist {} depend {}'.format(task_id, upstream_task_id))

        # todo 需要再继续是否需要增加外部依赖
        dag_id = None
        task_dep = DagTaskDependence(dag_id, task_id, upstream_task_id, type, trigger_rule)

        session.add(task_dep)
        session.commit()

        pass

    @classmethod
    @landsat_provide_session
    def external_add_task_dep(cls, dag_id, task_id, upstream_task_id, session=None):
        """
        配置任务依赖
        :param session: 数据库连接
        :param task_id: 任务id
        :param upstream_task_id: 依赖的任务id
        :param type: 用户添加类型 USER_DEP_TYPE,为默认值
        :return:
        """

        type = cls.AIRFLOW_DEP_TYPE

        TD = DagTaskDependence

        qry = session.query(DagTaskDependence).filter(
            # Task.dag_id == dag_id,
            TD.type == type,
            TD.task_id == task_id,
            TD.upstream_task_id == upstream_task_id,

        )

        task_dep = qry.first()

        # 如果存在就删除，每次取最新的
        if task_dep is not None:
            # raise Exception('Exist {} depend {}'.format(task_id, upstream_task_id))
            session.delete(task_dep)
            session.commit()

        task_dep = DagTaskDependence(dag_id, task_id, upstream_task_id, type)

        session.add(task_dep)
        session.commit()

        pass

    @classmethod
    @landsat_provide_session
    def add_airflow_task_dep(cls, dag_id, task_id, upstream_task_id, session=None):
        """
        配置任务依赖
        :param session: 数据库连接
        :param task_id: 任务id
        :param upstream_task_id: 依赖的任务id
        :param type: 用户添加类型 USER_DEP_TYPE,为默认值
        :return:
        """

        type = cls.AIRFLOW_DEP_TYPE

        TD = DagTaskDependence

        qry = session.query(DagTaskDependence).filter(
            # Task.dag_id == dag_id,
            TD.type == type,
            TD.task_id == task_id,
            TD.upstream_task_id == upstream_task_id,

        )

        task_dep = qry.first()

        # 如果存在就删除，每次取最新的
        if task_dep is not None:
            session.delete(task_dep)
            session.commit()

        task_dep = DagTaskDependence(dag_id, task_id, upstream_task_id, type)

        session.add(task_dep)
        session.commit()


    @classmethod
    @landsat_provide_session
    def external_get_task_up_depends(cls, task_id, session=None):
        """
        获取 task 是否有额外任务，就是影子任务
        判断依据，该任务的系统依赖的上游类型是否为影子任务类型，
        或者通过名称的方式 是否包含该名称的 加 wait 的组合
        :param task_id:
        :param session:
        :return:
        """

        type = cls.AIRFLOW_DEP_TYPE
        TD = DagTaskDependence

        qry = session.query(TD).filter(
            # Task.dag_id == dag_id,
            TD.task_id == task_id,
            TD.type == type

        )

        tds = qry.all()

        task_ids = []
        for td in tds:
            task_ids.append(td.upstream_task_id)

        return task_ids
        pass

    # 获取所有用户配置的任务依赖
    @classmethod
    @landsat_provide_session
    def get_all_user_task_dep(cls, session=None):

        type = cls.USER_DEP_TYPE

        TD = DagTaskDependence

        qry = session.query(DagTaskDependence).filter(
            # Task.dag_id == dag_id,
            TD.type == type,

        )

        task_deps = qry.all()

        task_dep_ids = []
        for task_dep in task_deps:
            task_dep_id = (task_dep.task_id, task_dep.upstream_task_id)

            task_dep_ids.append(task_dep_id)

        return task_dep_ids

    # 如果选择该依赖，是否有环存在
    @staticmethod
    @landsat_provide_session
    def upstreams_is_cyclic(task_id, upstream_task_ids, session=None):
        """
        判断选择任务，是否存在环
        :param session: 数据库连接
        :param task_id: 任务id
        :param upstream_task_ids: 依赖的任务 id 列表
        :return: 是否存在环
        """

        task_dep_ids = DagTaskDependence.get_all_user_task_dep()

        # 得到节点的个数
        task_id_dict = {}
        node_num = 0
        task_id_dict[task_id] = node_num
        for upstream_task_id in upstream_task_ids:
            if upstream_task_id not in task_id_dict.keys():
                node_num += 1
                task_id_dict[upstream_task_id] = node_num

        for task_dep_id in task_dep_ids:
            old_task_id, old_upstream_task_id = task_dep_id

            if old_task_id not in task_id_dict.keys():
                node_num += 1
                task_id_dict[old_task_id] = node_num

            if old_upstream_task_id not in task_id_dict.keys():
                node_num += 1
                task_id_dict[old_upstream_task_id] = node_num

        task_size = len(task_id_dict.keys())

        # 添加所有节点的关系
        g = Graph(task_size)
        for upstream_task_id in upstream_task_ids:
            g.addEdge(task_id_dict[task_id], task_id_dict[upstream_task_id])

        for task_dep_id in task_dep_ids:
            old_task_id, old_upstream_task_id = task_dep_id
            g.addEdge(task_id_dict[old_task_id], task_id_dict[old_upstream_task_id])

        # 判断
        if g.isCyclic():
            is_cyclic = True
        else:
            is_cyclic = False

        return is_cyclic

    # 如果选择该依赖，是否有环存在
    @staticmethod
    @landsat_provide_session
    def is_cyclic(task_id, upstream_task_id, session=None):
        """
        判断选择任务，是否存在环
        :param session: 数据库连接
        :param task_id: 任务id
        :param upstream_task_id: 依赖的任务 id
        :return: 是否存在环
        """

        task_dep_ids = DagTaskDependence.get_all_user_task_dep()

        # 得到节点的个数

        task_id_dict = {}
        node_num = 0
        task_id_dict[task_id] = node_num
        node_num += 1
        task_id_dict[upstream_task_id] = node_num

        for task_dep_id in task_dep_ids:
            old_task_id, old_upstream_task_id = task_dep_id

            if old_task_id not in task_id_dict.keys():
                node_num += 1
                task_id_dict[old_task_id] = node_num

            if old_upstream_task_id not in task_id_dict.keys():
                node_num += 1
                task_id_dict[old_upstream_task_id] = node_num

        task_size = len(task_id_dict.keys())

        # 添加所有节点的关系
        g = Graph(task_size)
        g.addEdge(task_id_dict[task_id], task_id_dict[upstream_task_id])

        for task_dep_id in task_dep_ids:
            old_task_id, old_upstream_task_id = task_dep_id

            g.addEdge(task_id_dict[old_task_id], task_id_dict[old_upstream_task_id])

        # 判断
        if g.isCyclic():
            is_cyclic = True
        else:
            is_cyclic = False

        return is_cyclic

    @staticmethod
    @landsat_provide_session
    def rename_task_id(task_id, new_task_id, session=None):
        TD = DagTaskDependence

        qry = session.query(TD).filter(
            TD.task_id == task_id,
        )
        qry.update({'task_id': new_task_id})

        session.commit()

    @staticmethod
    @landsat_provide_session
    def rename_upstream_task_id(upstream_task_id, new_upstream_task_id, session=None):
        TD = DagTaskDependence

        qry = session.query(TD).filter(
            TD.upstream_task_id == upstream_task_id,
        )
        qry.update({'upstream_task_id': new_upstream_task_id})

        session.commit()

    @staticmethod
    @landsat_provide_session
    def get_task_up_depends_list(task_id, session=None):

        type = DagTaskDependence.USER_DEP_TYPE
        TD = DagTaskDependence

        qry = session.query(TD).filter(
            # Task.dag_id == dag_id,
            TD.task_id == task_id,
            TD.type == type

        )

        tds = qry.all()

        return tds

    @classmethod
    @landsat_provide_session
    def has_up_task(cls, task_id, session=None):
        task_ids = DagTaskDependence.get_task_up_depends(id)

        if len(task_ids) == 0:
            return False
        else:
            return True

    @classmethod
    @landsat_provide_session
    def recursion_get_task_up_depends(cls, task_id, session=None):
        all_up_task_ids = []
        up_task_ids = cls.get_task_up_depends(task_id=task_id)
        all_up_task_ids += up_task_ids
        for up_task_id in up_task_ids:
            all_up_task_ids += cls.recursion_get_task_up_depends(up_task_id)
        return all_up_task_ids

