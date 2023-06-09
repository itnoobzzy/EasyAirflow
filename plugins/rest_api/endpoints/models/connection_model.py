#!/usr/bin/env python
# -*- coding:utf-8 -*-
from airflow.utils.session import provide_session
from sqlalchemy import Column, Integer, String, Boolean
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import synonym


from cryptography.fernet import Fernet, MultiFernet
from rest_api.config import FERNET_KEY

from rest_api.utils.airflow_database import Base, airflow_provide_session


def get_fernet():
    fernet = MultiFernet([
        Fernet(fernet_part.encode('utf-8'))
        for fernet_part in FERNET_KEY.split(',')
    ])

    return fernet


class AirflowConnection(Base):
    """
    Placeholder to store information about different database instances
    connection information. The idea here is that scripts use references to
    database instances (conn_id) instead of hard coding hostname, logins and
    passwords when using operators or hooks.
    """
    __tablename__ = "connection"

    id = Column(Integer(), primary_key=True)
    conn_id = Column(String(250))
    conn_type = Column(String(500))
    host = Column(String(500))
    schema = Column(String(500))
    login = Column(String(500))
    _password = Column('password', String(5000))
    port = Column(Integer())
    is_encrypted = Column(Boolean, unique=False, default=False)
    is_extra_encrypted = Column(Boolean, unique=False, default=False)
    _extra = Column('extra', String(5000))

    def __init__(
            self, conn_id=None, conn_type=None,
            host=None, login=None, password=None,
            schema=None, port=None, extra=None
    ):
        self.conn_id = conn_id

        self.conn_type = conn_type
        self.host = host
        self.login = login
        self.password = password
        self.schema = schema
        self.port = port
        self.extra = extra

    def get_password(self):
        if self._password and self.is_encrypted:
            fernet = get_fernet()
            return fernet.decrypt(bytes(self._password, 'utf-8')).decode()
        else:
            return self._password

    def set_password(self, value):
        if value:
            fernet = get_fernet()
            self._password = fernet.encrypt(bytes(value, 'utf-8')).decode()
            self.is_encrypted = True

    @declared_attr
    def password(cls):
        return synonym('_password',
                       descriptor=property(cls.get_password, cls.set_password))

    def get_extra(self):
        if self._extra and self.is_extra_encrypted:
            fernet = get_fernet()
            return fernet.decrypt(bytes(self._extra, 'utf-8')).decode()
        else:
            return self._extra

    def set_extra(self, value):
        if value:
            fernet = get_fernet()
            self._extra = fernet.encrypt(bytes(value, 'utf-8')).decode()
            self.is_extra_encrypted = True
        else:
            self._extra = value
            self.is_extra_encrypted = False

    @declared_attr
    def extra(cls):
        return synonym('_extra',
                       descriptor=property(cls.get_extra, cls.set_extra))

    def rotate_fernet_key(self):
        fernet = get_fernet()
        if self._password and self.is_encrypted:
            self._password = fernet.rotate(self._password.encode('utf-8')).decode()
        if self._extra and self.is_extra_encrypted:
            self._extra = fernet.rotate(self._extra.encode('utf-8')).decode()

    @staticmethod
    @provide_session
    def get_connection_by_conn_id(conn_id, session=None):


        qry = session.query(AirflowConnection).filter(
            AirflowConnection.conn_id == conn_id,
        )
        conn = qry.first()

        return conn

    @provide_session
    def save_connection(self, session=None):
        """
        如果存在就 merge
        如果不存在就 add
        :param session:
        :return:
        """
        conn_id = self.conn_id

        conn = AirflowConnection.get_connection_by_conn_id(conn_id)

        if conn is None:
            session.add(self)
        else:
            self.id = conn.id
            session.merge(self)
        pass
