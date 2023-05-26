from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from plugins.operators.utils.var_parse import VarParse


class LandsatMySqlOperator(BaseOperator):
    """
    Executes sql code in a specific MySQL database

    :param sql: the sql code to be executed. Can receive a str representing a
        sql statement, a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
        (templated)
    :type sql: str or list[str]
    :param mysql_conn_id: reference to a specific mysql database
    :type mysql_conn_id: str
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: mapping or iterable
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :type autocommit: bool
    :param database: name of database which overwrite defined one in connection
    :type database: str
    """

    # template_fields = ('sql',)
    # template_ext = ('.sql',)
    # ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self, sql, mysql_conn_id='mysql_default', parameters=None,
            autocommit=False, database=None, *args, **kwargs):
        super(LandsatMySqlOperator, self).__init__(*args, **kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.sql = sql
        self.autocommit = autocommit
        self.parameters = parameters
        self.database = database

    def execute(self, context):
        self.log.info('Executing: %s', self.sql)

        self.sql = VarParse.operator_re_replace_datetime_var(self.sql, context)

        self.log.info('Executing really: %s', self.sql)

        #  去掉末尾的 ; ,并对 sql 按照 ;\n 进行拆分
        self.sql = VarParse.split_sql(self.sql)
        self.log.info('Split SQL really: %s', self.sql)

        try:

            hook = MySqlHook(mysql_conn_id=self.mysql_conn_id, schema=self.database)

            hook.run(
                self.sql,
                autocommit=self.autocommit,
                parameters=self.parameters)

        except Exception as e:
            # self.log.error('Executing failed: {}', self.sql)
            self.log.error('Executing failed!')
            raise

        self.log.info("Done.")
