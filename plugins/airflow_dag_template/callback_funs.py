from datetime import datetime

from airflow_dag_template.sqlalchemy_util import props, provide_session

from airflow_dag_template.TaskDefine import TaskDefineModel

@provide_session
def get_task_define(dag_id,task_id,session=None):

    task_define = session.query(TaskDefineModel)\
        .filter(TaskDefineModel.task_id == task_id)\
        .first()

    return task_define



def context_base_info(context):
    print('wanglong-context : {}'.format(context))
    print('wanglong-type : {}'.format(type(context)))

    task_instance = None

    if 'task_instance' in context:
        task_instance = context['task_instance']

        object_dict = props(task_instance)

        print('task_instance : {}'.format(object_dict))


    if 'ti' in context:
        task_instance = context['ti']
        object_dict = props(task_instance)

        print('ti : {}'.format(object_dict))


    task_id = task_instance.task_id
    dag_id = None

    task_define = get_task_define(dag_id, task_id)

    owner = task_define.owner
    execution_date = task_instance.execution_date
    start_date = task_instance.start_date
    end_date = datetime.utcnow()
    duration = end_date - start_date.replace(tzinfo=None)
    log_url = task_instance.log_url
    state = task_instance.state


    # todo 如果错误，还有错误分析，这个需要后面才能做
    notice_dict = {
         'task_id' : task_id
        ,'owner' : owner
        ,'execution_date' : execution_date
        ,'execution_date_str' : execution_date.isoformat()
        ,'start_date' : start_date
        ,'start_date_str' : start_date.isoformat()
        ,'duration' : duration
        ,'duration_total_seconds' : duration.total_seconds()
        ,'log_url' : log_url
        ,'state' : state
        ,'end_date' : end_date
    }
    print('wanglong-notice_dict : {}'.format(notice_dict))



    # todo 需要根据 通知配置获取通知的规则

    return notice_dict


def on_failure_callback_fn(context):

    context_base_info(context)


    pass


def on_success_callback_fn(context):

    context_base_info(context)

    pass


def on_retry_callback_fn(context):

    context_base_info(context)

    pass


def sla_miss_callback_fn(dag, task_list, blocking_task_list, slas,
                                          blocking_tis):

    """
    文件
    ENV/lib/python3.8/site-packages/airflow/jobs/scheduler_job.py
    调用方式
    dag.sla_miss_callback(dag, task_list, blocking_task_list, slas,
                      blocking_tis)

    task 可以自定  sla=None,  # type: Optional[timedelta]

    每一个任务都可以定 sla  时间，通过判断延时，判断 sla 是否生效，然后触发
    sal 执行时间 + 超时时间 小于当前时间 就是超时了
    :param dag: 配置的 dag
    :param task_list: dag 的所有任务
    :param blocking_task_list: dag 所有的延时任务
    :param slas: 延时的 SlaMiss 对象
    :param blocking_tis: 延时的实例
    :return:
    """
    print('sla_miss_callback_fn =========>')
    print('sla_miss_callback_fn =========>dag : {}'.format(dag))
    print('sla_miss_callback_fn =========>task_list: {}'.format(task_list))
    print('sla_miss_callback_fn =========>blocking_task_list: {}'.format(blocking_task_list))
    print('sla_miss_callback_fn =========>slas: {}'.format(slas))
    print('sla_miss_callback_fn =========>blocking_tis: {}'.format(blocking_tis))

    pass



"""

[成功] 任务调度通知

任务名：@任务名@

任务状态：成功

责任人：@责任人账号@

详情：@任务名@计划调度时间为MM-DD HH:mm，已于MM-DD HH:mm执行成功，耗时X分X秒。

查看更多：@[link] 跳转至该任务的任务详情页@

"""

"""
[失败] 任务调度通知

任务名：@任务名@

任务状态：@任务状态@

责任人：@责任人账号@

报错原因：@任务详情-日志-错误分析@

详情：@任务名@调度失败，请点击以下链接查看日志定位错误原因。

查看更多：@[link] 跳转至该任务的任务详情-日志页@

"""

if __name__ == '__main__':



    pass