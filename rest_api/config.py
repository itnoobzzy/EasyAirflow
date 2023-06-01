import os

AIRFLOW_HOST = os.environ.get('AIRFLOW_HOST', 'http://airflow-webserver.akulaku.com')
AIRFLOW_USER = os.environ.get('AIRFLOW_USER', 'admin')
AIRFLOW_PWD = os.environ.get('AIRFLOW_PWD', 'admin')

DAGS_DIR = os.environ.get('DAGS_DIR')
AIRFLOW_UPLOAD_DAG_HOST = os.environ.get('AIRFLOW_UPLOAD_DAG_HOST', 'http://airflow-webserver.akulaku.com')
AIRFLOW_UPLOAD_DAG_PORT = os.environ.get('AIRFLOW_UPLOAD_DAG_PORT', '8080')

FERNET_KEY = os.environ.get('FERNET_KEY', 'FERNET_KEY')
SERVE_LOG_PROT = os.environ.get('SERVE_LOG_PROTOCOL', 'http')

AIRFLOW_DAGS_DIR = os.environ.get('AIRFLOW_DAGS_DIR', '/usr/local/airflow/dags')

AIRFLOW_SQLALCHEMY_URI = os.environ.get('AIRFLOW_SQLALCHEMY_URI', 'mysql://airflow:airflow@localhost:3306/airflow')

LANDSAT_SQLALCHEMY_URI = os.environ.get('LANDSAT_SQLALCHEMY_URI', 'mysql://airflow:airflow@localhost:3306/landsat')

