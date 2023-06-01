#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:wanglong
import json

import requests


class TaskWebHandlers(object):


    @staticmethod
    def direct_run_task_instance(host, port, dag_id, task_id, execution_date):
        url = 'http://{host}:{port}/api/v1/task/instance/run'.format(host=host, port=port)

        values = {'dag_id': dag_id
            , 'task_id': task_id
            , 'execution_date': execution_date
                  }

        r = requests.post(url, data=values)

        if r.status_code != 200:
            # raise Exception("run task instance {} {} failed \n error:{}".format(task_id, execution_date, r.content))
            json_err = {"task_id": task_id,
                        "execution_date": str(execution_date),
                        "content": str(r.content),
                        "status": r.status_code}
            raise Exception(json.dumps(json_err))
