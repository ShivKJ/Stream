from collections import deque
from concurrent.futures import (Executor, Future, ProcessPoolExecutor as PPE,
                                ThreadPoolExecutor as TPE, as_completed)
from functools import partial
from operator import itemgetter
from typing import Deque, Iterable

from streamAPI.stream.decos import cancel_remaining_jobs, check_stream
from streamAPI.stream.stream import Stream
from streamAPI.utility.Types import (Filter, Function, T, X)
from streamAPI.utility.utils import get_functions_clazz


class Exec(Stream[T]):
    def __init__(self, data: Iterable[T],
                 worker: int,
                 multiprocessing: bool = True):
        super().__init__(data)

        self._registered_jobs: Deque[Future] = deque()
        self._exec: Executor = (PPE if multiprocessing else TPE)(max_workers=worker)
        self._worker = worker

    def _func_wrapper(self: 'ParallelStream[T]', func, timeout=None) -> Stream[T]:
        """
        provides a wrapper around given function.
        :param self:
        :param func:
        :param timeout:
        :return:
        """
        stream = (Stream(self)
                  .map(partial(self._submit_job, func))
                  .peek(self._registered_jobs.append)
                  .batch(self._worker)
                  .map(as_completed)
                  .flat_map())

        if timeout is not None:
            result_extractor = partial(Future.result, timeout=timeout)
        else:
            result_extractor = Future.result

        return stream.map(result_extractor)

    def _submit_job(self, func, g) -> Future:
        return self._exec.submit(func, g)

    @staticmethod
    def _stop_all_jobs(func, self: 'Exec', *args, **kwargs):
        out = func(*args, **kwargs)

        for worker in self._registered_jobs:
            worker.cancel()

        return out


class ParallelStream(Exec[T]):
    def __init__(self, data: Iterable[T],
                 worker: int,
                 multiprocessing: bool = True):
        """
        Creates a parallel stream.

        :param data:
        :param worker: number of worker
        :param multiprocessing: it True then multiprocessing is used else multiThreading.
        """

        super().__init__(data, worker=worker, multiprocessing=multiprocessing)

    @check_stream
    def map_concurrent(self, func: Function[T, X], timeout=None) -> 'ParallelStream[T]':
        self._pointer = self._func_wrapper(func, timeout=timeout)
        return self

    @check_stream
    def filter_concurrent(self, predicate: Filter[T], timeout=None) -> 'ParallelStream[T]':
        def _predicate(g):
            return predicate(g), g

        self._pointer = (self._func_wrapper(_predicate, timeout=timeout)
                         .filter(itemgetter(0))
                         .map(itemgetter(1)))
        return self

    # terminal operation will trigger cancelling of submitted unnecessary jobs.
    partition = cancel_remaining_jobs(Stream.partition)
    count = cancel_remaining_jobs(Stream.count)
    min = cancel_remaining_jobs(Stream.min)
    max = cancel_remaining_jobs(Stream.max)
    group_by = cancel_remaining_jobs(Stream.group_by)
    mapping = cancel_remaining_jobs(Stream.mapping)
    as_seq = cancel_remaining_jobs(Stream.as_seq)
    all = cancel_remaining_jobs(Stream.all)
    any = cancel_remaining_jobs(Stream.any)
    none_match = cancel_remaining_jobs(Stream.none_match)
    find_first = cancel_remaining_jobs(Stream.find_first)
    reduce = cancel_remaining_jobs(Stream.reduce)
    done = cancel_remaining_jobs(Stream.done)
    for_each = cancel_remaining_jobs(Stream.for_each)
    __iter__ = cancel_remaining_jobs(Stream.__iter__)


if __name__ == 'streamAPI.stream.parallelStream':
    __all__ = get_functions_clazz(__name__, __file__)
