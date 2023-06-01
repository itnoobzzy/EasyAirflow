"""
引入 akcensus 日志模块, 需要放在最前面
"""
import logging
try:
    import akcensus
    akcensus.setup('airflow-plus', current_frame_depth=2)
except Exception as e:
    logging.error(str(e))

from flask import Flask
from flask_restful import Api

from endpoints.resources import (
    IdResource,
    ConnectionResource,
    DagResource,
    DagRunListResource,
    DagRunPlanExecutionDateResource,
    DagRunTaskInstancesResource,
    DagRunSummaryResource,
    TaskInstanceListResource,
    TaskInstanceSummaryResource,
    TaskInstanceLogResource,
    TaskInstanceResource,
    DagRunLastResource,
    DagRunResource,
    DagFile
)

app = Flask(__name__)
api = Api(app)

api.prefix = '/api/v1'

api.add_resource(IdResource, '/id')
api.add_resource(ConnectionResource, '/connections')

api.add_resource(DagResource, '/dags')
api.add_resource(DagFile, '/dagfile')

api.add_resource(DagRunPlanExecutionDateResource, '/dagruns/plan_execution_date')
api.add_resource(DagRunTaskInstancesResource, '/dagruns/task_instances')
api.add_resource(DagRunLastResource, '/dagruns/latest')
api.add_resource(DagRunSummaryResource, '/dagruns/summary')
api.add_resource(DagRunListResource, '/dagruns')
api.add_resource(DagRunResource, '/dagrun')

api.add_resource(TaskInstanceSummaryResource, '/task_instances/summary')
api.add_resource(TaskInstanceListResource, '/task_instances')
api.add_resource(TaskInstanceLogResource, '/task_instance/log/<task_id>')
api.add_resource(TaskInstanceResource, '/task_instance/<task_id>', '/task_instance')

if __name__ == '__main__':
    app.run(debug=True, port=8080, host="0.0.0.0")
