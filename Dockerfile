FROM apache/airflow:slim-latest-python3.10
USER root
EXPOSE 8080 5555 8793
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

RUN chmod 777 -R /opt/airflow/logs
RUN chmod 777 -R /opt/airflow/dags
USER airflow
COPY config/airflow.cfg /opt/airflow/airflow.cfg

RUN pip install celery
RUN pip install flower
RUN pip install pymysql
RUN pip install mysqlclient
RUN pip install redis
RUN pip install livy==0.6.0
RUN pip install apache-airflow-providers-mysql
RUN pip install apache-airflow-providers-apache-hive
RUN pip install loguru==0.5.1
RUN pip install opencensus
RUN pip install opencensus-ext-requests
RUN pip install opencensus-ext-sqlalchemy
RUN pip install pathlib2
RUN pip install Flask-RESTful

RUN airflow db init
