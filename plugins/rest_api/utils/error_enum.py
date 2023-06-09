from enum import Enum


class ErrorEnum(Enum):
    """统一错误码"""

    # 没有数据源连接权限错误
    NO_DATA_SOURCE_CONN_AUTH = 999, {'error': "没有该数据源规则权限"}
    NO_UPDATE_OWNER_AUTH = 1000, {'error': "只有责任人可以修改责任人"}
    GET_CONN_IDS_ERROR = 1001, {'error': "获取有权限数据源链接ID错误"}

    # 规则模板处理错误码
    RULE_TEMPLATE_EXITS = 11000, {'error': '该规则模板名称已存在'}
    RULE_TEMPLATE_INVOKED = 11001, {'error': "存在规则引用该规则模板"}
    TEMPLATE_SQL_INVALID = 11002, {'error': "模板SQL格式不正确"}

    # 规则处理错误码
    NOT_OWNER = 12001, {'error': '非责任人无法删除或修改'}
    IS_PUBLISHED = 12002, {'error': '规则已上线无法删除'}
    RULE_CONFIG_MISS = 12003, {'error': '规则校验配置不完善'}
    RULE_INFO_NONE = 12004, {'error': '规则校验规则信息获取为空'}
    BATCH_DELETE_ERR = 12005, {'error': '批量删除错误'}
    EXECUTE_CALLBACK_ERR = 12006, {'error': '执行回调错误'}
    ALERT_CALLBACK_ERR = 12007, {'error': '执行告警回调错误'}
    NO_SAMPLE_INFO_ERR = 12008, {'error': '查询样例数据失败'}

    # 元数据系统错误
    METADATA_ERROR = 20001, {'error': '元数据服务错误, 请联系管理员!'}

    # landsat系统错误
    LANDSAT_ERROR = 30001, {'error': 'landsat服务错误, 请联系管理员!'}

    # presto 系统错误
    PRESTO_ERROR = 40001, {'error': 'presto服务错误, 请联系管理员!'}

    # event 系统错误
    EVENT_ERROR = 50001, {'error': 'event服务错误, 请联系管理员!'}

    # airflow 服务错误
    AIRFLOW_ERROR = 60001, {'error': 'airflow 服务错误, 请联系管理员!'}