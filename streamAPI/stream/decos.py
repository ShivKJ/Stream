from typing import TypeVar

from decorator import decorator

from streamAPI.stream.exception import StreamClosedException

T = TypeVar('T')


def _raise(is_closed: bool):
    """
    throws exception depending on is_closed
    :param is_closed:
    """
    if is_closed:
        raise StreamClosedException()


@decorator
def check_stream(func, *args, **kwargs):
    """
    If Stream is closed then throws an exception otherwise,
    execute the function.
    :param func:
    :return:
    """
    _raise(args[0].closed)  # args[0] corresponds to self
    return func(*args, **kwargs)


@decorator
def close_stream(func, *args, **kwargs):
    """
    closes stream after executing the function.
    :param func:
    :return:
    """
    out = func(*args, **kwargs)
    args[0].closed = True  # args[0] corresponds to self
    return out
