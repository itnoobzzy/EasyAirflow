FROM apache/airflow:slim-latest-python3.10
USER root
EXPOSE 8080 5555 8793
COPY config/airflow.cfg /opt/airflow/airflow.cfg
RUN set -ex \
    && buildDeps=' \
        freetds-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        git \
        python3-dev \
        gcc \
        sasl2-bin \
        libsasl2-2 \
        libsasl2-dev \
        libsasl2-modules \
    ' \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        freetds-bin \
        build-essential \
        default-libmysqlclient-dev \
        apt-utils \
        curl \
        rsync \
        netcat \
        locales \
        procps \
        telnet

USER airflow
RUN pip install celery
RUN pip install flower
RUN pip install pymysql
RUN pip install mysqlclient
RUN pip install redis
RUN pip install livy==0.6.0
RUN pip install apache-airflow-providers-mysql
RUN pip install apache-airflow-providers-apache-hive

RUN airflow db init