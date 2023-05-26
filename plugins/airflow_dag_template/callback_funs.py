from datetime import datetime
import logging

from airflow_dag_template.sqlalchemy_util import props
from airflow_dag_template.TaskDefine import TaskDefineModel


logger = logging.getLogger(__name__)


def context_base_info(context):
    task_instance = None

    if 'ti' in context:
        task_instance = context['ti']
        object_dict = props(task_instance)
        logger.info('ti : {}'.format(object_dict))

    task_id = task_instance.task_id
    dag_id = None

    task_define = TaskDefineModel.get_task_define(dag_id, task_id)
    owner = task_define.owner
    execution_date = task_instance.execution_date
    start_date = task_instance.start_date
    end_date = datetime.utcnow()
    duration = end_date - start_date.replace(tzinfo=None)
    log_url = task_instance.log_url
    state = task_instance.state
    notice_dict = {
        'task_id': task_id
        , 'owner': owner
        , 'execution_date': execution_date
        , 'execution_date_str': execution_date.isoformat()
        , 'start_date': start_date
        , 'start_date_str': start_date.isoformat()
        , 'duration': duration
        , 'duration_total_seconds': duration.total_seconds()
        , 'log_url': log_url
        , 'state': state
        , 'end_date': end_date
    }
    logger.info('TI details: {}'.format(notice_dict))
    return notice_dict


def on_failure_callback_fn(context):
    context_base_info(context)


def on_success_callback_fn(context):
    context_base_info(context)


def on_retry_callback_fn(context):
    context_base_info(context)


def sla_miss_callback_fn(dag, task_list, blocking_task_list, slas,
                         blocking_tis):
    pass

