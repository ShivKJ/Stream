"""
author: Shiv
email: shivkj001@gmail.com
"""

from collections import deque
from concurrent.futures import (Executor, Future, ProcessPoolExecutor as PPE,
                                ThreadPoolExecutor as TPE, as_completed)
from functools import partial, wraps
from operator import itemgetter
from os import cpu_count
from typing import Deque, Iterable, Tuple

from streamAPI.stream.decos import check_pipeline, close_pipeline
from streamAPI.stream.stream import Stream
from streamAPI.utility.Types import Consumer, Filter, Function, T, X
from streamAPI.utility.utils import get_functions_clazz


class Exec(Stream[T]):
    def __init__(self, data: Iterable[T],
                 worker: int = None,
                 multiprocessing: bool = True):
        """
        Initialises executor which will be used for processing stream elements.

        If "worker" is None, then in case of multiprocessing number of cpu in
        the system is used and in case of multiThreading (i.e multiprocessing = False)
        "worker" is 5 * cpu_count().

        :param data:
        :param worker: number of worker
        :param multiprocessing: it True then multiprocessing is used else multiThreading.
        """

        super().__init__(data)

        self._registered_jobs: Deque[Future] = deque()

        if worker is None:
            worker = Exec._default_worker(multiprocessing)

        self._exec: Executor = (PPE if multiprocessing else TPE)(max_workers=worker)
        self._worker = worker

    @staticmethod
    def _default_worker(multiprocessing: bool):
        """
        returns number of worker for concurrent processing of stream elements.

        In case of multiprocessing, it is number of cpu_count() and for
        multiThreading it is 5 times cpu_count().

        :param multiprocessing:
        :return:
        """

        worker = cpu_count() or 1

        return worker if multiprocessing else 5 * worker

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
                 worker: int = None,
                 multiprocessing: bool = True):
        """
        Initialises executor which is used for concurrently processing
        stream elements.

        If "worker" is None, then in case of multiprocessing number of cpu in
        the system is used and in case of multiThreading (i.e multiprocessing = False)
        "worker" is 5 * cpu_count().

        :param data:
        :param worker: number of worker
        :param multiprocessing: it True then multiprocessing is used else multiThreading.
        """

        super().__init__(data=data, worker=worker, multiprocessing=multiprocessing)

    @staticmethod
    def _batch_process(func: Function[T, X], gs: Iterable[T]) -> Tuple[X, ...]:
        """
        Applies function "func" on data points "gs" and returns tuple of
        transformed data points.

        :param func: transforming function
        :param gs:

        :return:
        """

        return tuple(func(g) for g in gs)

    @check_pipeline
    def batch_processor(self, func: Function[T, X], dispatch_size: int, timeout=None):
        """
        This method is advised to be invoked when using MultiProcessing.

        Usually dispatching single unit of work (that is applying function on
        a element) is less costly. So it is better to send a batch of elements to
        a worker. Here "dispatch_size" is size of dispatch i.e. this many number
        of stream elements will be sent to each processor(worker) in one go.

        :param func:
        :param dispatch_size: number of stream elements to be given to each worker
                              in one go.
        :param timeout: time to wait for task to be done, if None then there is no
                        limit on execution time.
        :return:
        """

        return (self.batch(dispatch_size)
                .map_concurrent(partial(ParallelStream._batch_process, func), timeout=timeout)
                .flat_map())

    @check_pipeline
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

    @check_pipeline
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
    count = Exec._stop_all_jobs(Stream.count)
    min = Exec._stop_all_jobs(Stream.min)
    max = Exec._stop_all_jobs(Stream.max)
    all = Exec._stop_all_jobs(Stream.all)
    any = Exec._stop_all_jobs(Stream.any)
    none_match = Exec._stop_all_jobs(Stream.none_match)
    find_first = Exec._stop_all_jobs(Stream.find_first)
    reduce = Exec._stop_all_jobs(Stream.reduce)
    for_each = Exec._stop_all_jobs(Stream.for_each)
    done = Exec._stop_all_jobs(Stream.done)
    collect = Exec._stop_all_jobs(Stream.collect)
    __iter__ = Exec._stop_all_jobs(Stream.__iter__)

    @Exec._stop_all_jobs
    @close_pipeline
    @check_pipeline
    def for_each_concurrent(self, consumer: Consumer[X], timeout=None, batch_size=None):
        """
        This method differs from "for_each" method in the way that it consumes
        stream elements concurrently.

        Example:
            from time import sleep
            from streamAPI.test.testHelper import random

            rnd = random(seed=None)

            def process(e):
                sleep(rnd.random())
                print(e)

            ParallelStream(range(10)).for_each_concurrent(process)

        :param consumer: function to consume stream elements.
        :param timeout: if None, there is no limit on execution time.
        :param batch_size: If it is None then number of worker is used.
        """

        for _ in self._parallel_processor(consumer, timeout=timeout, batch_size=batch_size):
            pass


if __name__ == 'streamAPI.stream.parallelStream':
    __all__ = get_functions_clazz(__name__, __file__)
