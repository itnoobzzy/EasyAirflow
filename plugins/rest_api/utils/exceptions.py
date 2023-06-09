import inspect


class EasyAirflowException(Exception):
    """
    Base class for EasyAirflowException.
    Each custom exception should be derived from this class
    """
    code = 500
    error = None


class ConnectionsException(EasyAirflowException):
    """Raise when add connections failed"""
    code = 1301
    error = 'add connections failed'


class DagsException(EasyAirflowException):
    """Raise deal with dags failed"""
    code = 1302
    error = 'deal with dags failed'


EasyAirflowExceptions = {
    o.code: o for o in globals().values()
    if inspect.isclass(o) and issubclass(o, EasyAirflowException)
}


if __name__ == '__main__':
    print(EasyAirflowExceptions)
