from collections import deque
from concurrent.futures import Executor, Future, ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from functools import wraps
from operator import itemgetter
from typing import Deque, Iterable, Tuple, TypeVar

from decorator import decorator

from streamAPI.stream.decos import cancel_remaining_jobs, function_wrapper
from streamAPI.stream.stream import Stream
from streamAPI.utility.utils import divide_in_chunk, filter_transform, get_functions_clazz

T = TypeVar('T')


@decorator
def _use_exec(func, *args, **kwargs):
    self: 'ParallelStream' = args[0]
    processing_func = args[1]

    if kwargs['use_exec'] and self._exec is not None:
        wrapped_func = function_wrapper(self, processing_func, kwargs.get('single_chunk', False))
        self._pointer = wrapped_func(self._pointer)
        processing_func = Future.result

    return getattr(Stream, func.__name__)(self, processing_func)


class ParallelStream(Stream[T]):
    def __init__(self, data: Iterable[T]):
        super().__init__(data)
        self._concurrent_worker: Deque[Future] = deque()
        self._exec: Executor = None
        self._worker: int = None

    def concurrent(self, worker, is_parallel) -> 'ParallelStream[T]':
        if self._exec is not None:
            raise AttributeError('Executor was initialized befores.')

        self._worker = worker
        self._exec = ProcessPoolExecutor if is_parallel else ThreadPoolExecutor
        self._exec = self._exec(max_workers=worker)

        return self

    @_use_exec
    def map(self, func, *, use_exec=True) -> 'ParallelStream':
        pass

    def _predicate_wrapper(self, predicate):
        def _predicate(g):
            return predicate(g), g

        @wraps(predicate)
        def f(generator: Iterable[T]):
            generator = divide_in_chunk(generator, self._worker)

            for gs in generator:
                container: Tuple[Future] = tuple(self._exec.submit(_predicate, g) for g in gs)
                self._concurrent_worker.extend(container)

                yield from filter_transform(map(Future.result, as_completed(container)),
                                            itemgetter(0), itemgetter(1))

        return f

    def filter(self, predicate, *, use_exec=True) -> 'ParallelStream':
        if use_exec and self._exec is not None:
            predicate = self._predicate_wrapper(predicate)

        return super().filter(predicate)

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
    __iter__ = cancel_remaining_jobs(Stream.__iter__)

    @cancel_remaining_jobs
    @_use_exec
    def for_each(self, consumer, *, use_exec=True, single_chunk=True):
        pass


if __name__ == 'stream.parallelStream':
    __all__ = get_functions_clazz(__name__, __file__)
