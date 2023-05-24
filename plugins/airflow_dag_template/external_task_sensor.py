from datetime import datetime
from croniter import croniter

from airflow_dag_template.TaskDefine import TaskDefineModel
from airflow_dag_template.sqlalchemy_util import pendulum_convert_datetime, datetime_convert_pendulum, provide_session


@provide_session
def get_task_scheduler_interval(dag_id, task_id, session=None):
    task_define = session.query(TaskDefineModel) \
        .filter(TaskDefineModel.task_id == task_id) \
        .first()

    cron_string_up = task_define.schedule_interval
    return cron_string_up


def is_up_eq_down_plan_exec_date(up_interval, down_interval, down_execution_date):
    """
    判断上下游任务计划执行时间是否重合
    :param up_interval: 上游任务调度周期
    :param down_interval: 下游任务调度周期
    :param down_execution_date: 下游任务实际执行时间
    :return: bool
    """
    # up_interval = '48 9 */1 * * 0'
    # down_interval = '48 9,10,11,12,13,14,15 * * * 0'
    # down_execution_date = '2021-09-26 15:48:00'
    # down_execution_date = time.strptime(down_execution_date, "%Y-%m-%d %H:%M:%S")
    # down_execution_date = int(time.mktime(down_execution_date))

    iter_down = croniter(down_interval, down_execution_date)
    down_plan_execution_date = iter_down.get_next()

    up_pre = croniter(up_interval, down_plan_execution_date)
    up_pre_plan_execution_date = up_pre.get_prev()

    up_pre_down = croniter(up_interval, up_pre_plan_execution_date)
    up_plan_execution_date = up_pre_down.get_next()
    if down_plan_execution_date == up_plan_execution_date:
        return True


def landsat_execution_date_fn(logical_date, **kwargs):
    execution_date = pendulum_convert_datetime(logical_date)

    print('landsat-execution_date : {}'.format(execution_date))
    print('landsat-type : {}'.format(type(execution_date)))
    external_dag_id = kwargs['external_dag_id']
    external_task_id = kwargs['external_task_id']
    dag_id = kwargs['ti'].dag_id
    task_id = kwargs['ti'].task_id
    print('landsat-external_dag_id : {}'.format(external_dag_id))
    print('landsat-external_task_id : {}'.format(external_task_id))

    cron_string_self = get_task_scheduler_interval(dag_id=dag_id, task_id=task_id)
    print('landsat-cron_string_self : {}'.format(cron_string_self))

    cron_string_up = get_task_scheduler_interval(dag_id=external_dag_id, task_id=external_task_id)
    print('landsat-cron_string_up : {}'.format(cron_string_up))

    if cron_string_self == cron_string_up:
        execution_date_up = datetime_convert_pendulum(execution_date)

        return execution_date_up

    # 获取当然任务的开时间
    start_time = execution_date

    # 获取当前任务的计划执行时间
    iter_self = croniter(cron_string_self, start_time)
    next_execution_date_self = iter_self.get_next(datetime)

    # 判断下游任务计划执行时间是否与上游任务计划执行时间重合
    eq = is_up_eq_down_plan_exec_date(cron_string_up, cron_string_self, start_time)
    if eq:
        # 如果相等，上游任务的计划执行时间就是当前任务的计划执行时间
        next_execution_date_up = next_execution_date_self

        iter_up = croniter(cron_string_up, next_execution_date_up)

        execution_date_up = iter_up.get_prev(datetime)
    else:
        # 如果不相等，上游任务的计划执行时间就是以当前任务为开始时间，上游任务interval的cro的上一次调度时间
        iter_up = croniter(cron_string_up, next_execution_date_self)

        next_execution_date_up = iter_up.get_prev(datetime)

        execution_date_up = iter_up.get_prev(datetime)

    print('landsat-execution_date_self : {}'.format(execution_date))
    print('landsat-next_execution_date_self : {}'.format(next_execution_date_self))
    print('landsat-next_execution_date_up : {}'.format(next_execution_date_up))
    print('landsat-execution_date_up : {}'.format(execution_date_up))

    execution_date_up = datetime_convert_pendulum(execution_date_up)

    return execution_date_up


if __name__ == '__main__':
    # external_task_id = 't1'
    # external_dag_id = 'dag-t1'
    # execution_date = datetime.now()
    #
    # cron_string_up = "*/10 */8 * * *"
    # cron_string_down = "* */3 * * *"
    #
    # start_time=datetime.now()
    #
    # iter_down=croniter(cron_string_down,start_time)
    # next_execution_date_down = iter_down.get_next(datetime)
    #
    # # print(next_execution_date_down)
    #
    # execution_date = next_execution_date_down
    # print(execution_date)
    #
    # execution_date_up = landsat_execution_date_fn(execution_date, external_dag_id, external_task_id)
    #
    #
    # print(execution_date_up)
    pass
