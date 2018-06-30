from collections import deque
from concurrent.futures import (Executor, Future, ProcessPoolExecutor as PPE,
                                ThreadPoolExecutor as TPE, as_completed)
from functools import partial, wraps
from operator import itemgetter
from typing import Deque, Iterable

from streamAPI.stream.decos import check_stream
from streamAPI.stream.stream import Stream
from streamAPI.utility.Types import (Filter, Function, T, X)
from streamAPI.utility.utils import get_functions_clazz


class Exec(Stream[T]):
    def __init__(self, data: Iterable[T],
                 worker: int,
                 multiprocessing: bool = True):
        """
        :param data:
        :param worker: number of worker
        :param multiprocessing: it True then multiprocessing is used else multiThreading.
        """
        super().__init__(data)

        self._registered_jobs: Deque[Future] = deque()
        self._exec: Executor = (PPE if multiprocessing else TPE)(max_workers=worker)
        self._worker = worker

    def _parallel_processor(self: 'ParallelStream[T]', func, timeout=None, batch_size=None) -> Stream[T]:
        """
        processes data points concurrently. Elements are processed in batches of size "batch_size".

        :param self:
        :param func:
        :param timeout: time to wait for task to be done, if None then there is no
                        limit on execution time.
        :param batch_size: If it is None then number of worker is used.
        :return:
        """

        batch_size = batch_size or self._worker
        assert batch_size > 0, 'Batch size must be positive.'

        stream = (Stream(iter(self._pointer))
                  .map(partial(self._submit_job, func))
                  .peek(self._registered_jobs.append)
                  .batch(batch_size)
                  .map(as_completed)
                  .flat_map())

        if timeout is not None:
            result_extractor = partial(Future.result, timeout=timeout)
        else:
            result_extractor = Future.result

        return stream.map(result_extractor)

    def _submit_job(self, func, g) -> Future:
        """
        Submits job to executor.

        :param func:
        :param g:
        :return:
        """
        return self._exec.submit(func, g)

    @staticmethod
    def _stop_all_jobs(terminal_op):
        """
        creates decorator, to terminate all waiting or redundant running jobs.

        :param terminal_op:
        :return:
        """

        @wraps(terminal_op)
        def f(self: 'Exec', *args, **kwargs):
            out = terminal_op(self, *args, **kwargs)

            for worker in self._registered_jobs:
                worker.cancel()

            return out

        return f


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

    @staticmethod
    def _batch_process(func: Function[T, X], gs: Iterable[T]) -> Iterable[X]:
        """
        Applies function "func" on data points "gs" and returns
        transformed list of data points.

        :param func:
        :param gs:

        :return:
        """

        return tuple(func(g) for g in gs)

    @check_stream
    def batch_processor(self, func: Function[T, X], n: int, timeout=None):
        """
        This method is advised to be invoked when using MultiProcessing.

        Usually dispatching single unit of work (that is applying function on
        a element) is less costly. So it is better to send batch of data to
        a worker. Here n is dispatch size i.e. this many number of stream
        elements will be sent to each processor(worker) in one go. Note that
        number of worker used will be same as "_worker".

        :param func:
        :param n:
        :param timeout: time to wait for task to be done, if None then there is no
                        limit on execution time.
        :return:
        """
        return (self.batch(n)
                .map_concurrent(partial(ParallelStream._batch_process, func), timeout=timeout)
                .flat_map())

    @check_stream
    def map_concurrent(self, func: Function[T, X], timeout=None, batch_size=None) -> 'ParallelStream[T]':
        """
        maps elements concurrently. Elements are processed in batches of size "batch_size".

        :param func:
        :param timeout: time to wait for task to be done, if None then there is no
                        limit on execution time.
        :param batch_size: If it is None then number of worker is used.
        :return:
        """
        self._pointer = self._parallel_processor(func, timeout=timeout, batch_size=batch_size)
        return self

    @check_stream
    def filter_concurrent(self, predicate: Filter[T], timeout=None, batch_size=None) -> 'ParallelStream[T]':
        """
        filters elements concurrently. Elements are processed in batches of size "batch_size".

        :param predicate:
        :param timeout: time to wait for task to be done, if None then there is no
                        limit on execution time.
        :param batch_size: If it is None then number of worker is used.
        :return:
        """

        def _predicate(g):
            return predicate(g), g

        self._pointer = (self._parallel_processor(_predicate, timeout=timeout, batch_size=batch_size)
                         .filter(itemgetter(0))
                         .map(itemgetter(1)))
        return self

    # terminal operation will trigger cancelling of submitted unnecessary jobs.
    partition = Exec._stop_all_jobs(Stream.partition)
    count = Exec._stop_all_jobs(Stream.count)
    min = Exec._stop_all_jobs(Stream.min)
    max = Exec._stop_all_jobs(Stream.max)
    group_by = Exec._stop_all_jobs(Stream.group_by)
    mapping = Exec._stop_all_jobs(Stream.mapping)
    as_seq = Exec._stop_all_jobs(Stream.as_seq)
    all = Exec._stop_all_jobs(Stream.all)
    any = Exec._stop_all_jobs(Stream.any)
    none_match = Exec._stop_all_jobs(Stream.none_match)
    find_first = Exec._stop_all_jobs(Stream.find_first)
    reduce = Exec._stop_all_jobs(Stream.reduce)
    done = Exec._stop_all_jobs(Stream.done)
    for_each = Exec._stop_all_jobs(Stream.for_each)
    __iter__ = Exec._stop_all_jobs(Stream.__iter__)


if __name__ == 'streamAPI.stream.parallelStream':
    __all__ = get_functions_clazz(__name__, __file__)
