import logging
from datetime import datetime, timedelta

from croniter import croniter
from flask_restful import Resource, reqparse, request

from endpoints.handlers.airflow_task_handlers import TaskHandlers
from endpoints.handlers.task_define_handlers import TaskDefineHandlers
from endpoints.models.dagrun_model import DagRun
from endpoints.models.task_instance_model import TaskInstance
from endpoints.models.task_model import TaskDefine
from endpoints.models.taskinstance_next_model import TaskInstanceNext
from endpoints.models.taskinstance_type_model import TaskInstanceType
from utils.response_safe import safe
from utils.times import dt2ts

class TaskInstanceSummaryResource(Resource):

    # todo
    @safe
    def get(self):

        parser = reqparse.RequestParser()

        parser.add_argument('task_ids',type=str,action="append")
        parser.add_argument('states',type=str,action="append")
        parser.add_argument('instance_type',type=str)
        parser.add_argument('min_plan_execution_date', type=int)
        parser.add_argument('max_plan_execution_date', type=int)


        args = parser.parse_args()

        task_ids = args['task_ids']
        states = args['states']
        instance_type = args['instance_type']
        min_execution_date_timestamp = args['min_plan_execution_date']
        max_execution_date_timestamp = args['max_plan_execution_date']

        # 将 NULL 转换为 None,统一为运行时状态
        if states and 'NULL' in states :
            states.append(None)
            states.remove('NULL')

        # if task_id is None \
        #         or start_plan_execution_date is None \
        #         or end_plan_execution_date is None:
        #     raise Exception("task_id or start_plan_execution_date or end_plan_execution_date can not be None！")

        instance_type_list = None
        if instance_type == '0':
            instance_type_list = [TaskInstanceType.FILL_DATA_TYPE, TaskInstanceType.CYCLE_TYPE]
        elif instance_type == '1':
            instance_type_list = [TaskInstanceType.CYCLE_TYPE]
        elif instance_type == '2':
            instance_type_list = [TaskInstanceType.FILL_DATA_TYPE]
        else:
            instance_type_list = [TaskInstanceType.FILL_DATA_TYPE, TaskInstanceType.CYCLE_TYPE]

        min_plan_execution_date = None
        max_plan_execution_date = None
        if isinstance(min_execution_date_timestamp, int):
            min_plan_execution_date = datetime.utcfromtimestamp(min_execution_date_timestamp / 1000)
        if isinstance(max_execution_date_timestamp, int):
            max_plan_execution_date = datetime.utcfromtimestamp(max_execution_date_timestamp / 1000)

        state_num = TaskDefineHandlers.get_task_instance_summary(
            task_ids,
            states,
            min_plan_execution_date,
            max_plan_execution_date,
            instance_type_list,
        )

        response_data = {
            'status': 200,
            "data": state_num

        }
        http_status_code = 200
        return response_data, http_status_code


    pass