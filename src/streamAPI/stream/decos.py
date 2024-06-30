"""
author: Shiv
email: shivkj001@gmail.com
"""

from decorator import decorator

from streamAPI.stream.exception import PipelineClosed


def _raise(is_closed: bool):
    """
    throws exception if is_closed is True
    :param is_closed:
    """

    if is_closed:
        raise PipelineClosed()


@decorator
def check_pipeline(func, *args, **kwargs):
    """
    If Stream is closed then throws an exception otherwise,
    execute the function.
    :param func:
    :return:
    """

    _raise(args[0].closed)  # args[0] corresponds to self
    return func(*args, **kwargs)


@decorator
def close_pipeline(func, *args, **kwargs):
    """
    closes stream after executing the function.
    :param func:
    :return:
    """

    out = func(*args, **kwargs)
    args[0].closed = True  # args[0] corresponds to self
    return out
