
# 对 task_instance 增加补丁 ,方法失败
利用了模型的一个特点，python 中的 sqlalchemy 的模型 可以比 数据库中的少字段

需要增加字段：
```sql
alter table task_instance add next_execution_date timestamp(6);
```

如果想回滚，则需要执行删除字段：
```sql
alter table task_instance drop next_execution_date ;
```

可能需要，对 next_execution_date 补齐
```sql
update task_instance set next_execution_date = execution_date
```


# 新增一个模型记录下一次执行时间


## 模型
```sql

CREATE TABLE `task_instance_next` (
  `task_id` varchar(250) NOT NULL,
  `dag_id` varchar(250) NOT NULL,
  `execution_date` timestamp(6) NOT NULL,
  `next_execution_date` timestamp(6) NOT NULL,
  PRIMARY KEY (`task_id`,`dag_id`,`execution_date`,`next_execution_date`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1

create unique index uni_key on task_instance_next (`task_id`,`dag_id`,`execution_date`) ;



CREATE TABLE `dag_run_next` (
  `dag_id` varchar(250) NOT NULL,
  `execution_date` timestamp(6) NOT NULL,
  `next_execution_date` timestamp(6) NOT NULL,
  PRIMARY KEY (`dag_id`,`execution_date`,`next_execution_date`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1

```


## dagrun.py
dagrun.py 的 398 行添加如下代码：

ENV/lib/python3.8/site-packages/airflow/models/dagrun.py



```python
ti_next = TaskInstanceNext(task, self.execution_date)
session.add(ti_next)
```

### 不存在的文件不好挂载，所以写到一个文件中去了