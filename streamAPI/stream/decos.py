from concurrent.futures import Future, as_completed
from functools import wraps
from typing import Iterable, Tuple, TypeVar

from decorator import decorator

from streamAPI.stream.exception import StreamClosedException
from streamAPI.utility.utils import divide_in_chunk

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


# ----------------------Parallel Streams------------------------------
@decorator
def cancel_remaining_jobs(func, *args, **kwargs):
    out = func(*args, **kwargs)
    for worker in args[0]._concurrent_worker:  # args[0] corresponds to self
        worker.cancel()

    return out


def function_wrapper(self, func, single_chunk=False):
    """
    provides a wrapper around given function.
    :param self:
    :param func:
    :param single_chunk: if False, the run jobs in chunks defined as self._worker else all
            jobs are submitted.
    :return:
    """

    @wraps(func)
    def f(generator: Iterable[T]):
        if single_chunk is False:
            generator = divide_in_chunk(generator, self._worker)
        else:
            generator = (generator,)

        for gs in generator:
            container: Tuple[Future] = tuple(self._exec.submit(func, g) for g in gs)
            self._concurrent_worker.extend(container)
            yield from as_completed(container)

    return f
