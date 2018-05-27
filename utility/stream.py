from concurrent import futures
from concurrent.futures import Future, as_completed
from functools import wraps
from itertools import islice, chain
from os import cpu_count
from typing import Iterable, TypeVar, Generic, Sequence, Dict, Any

from utility.utils import get_functions_clazz, identity, divide_in_chunk

T = TypeVar('T')


class Optional(Generic[T]):
    """
    This class wraps data. This helps avoid processing None element.
    """

    def __init__(self, data: T):
        self.data = data

    def present(self):
        return self.data is not None

    def get(self):
        if not self.present():
            raise ValueError('data is None')

        return self.data

    def if_present(self, consumer):
        if self.present():
            consumer(self.data)

    def or_else(self, other):
        return self.data if self.present() else other

    def or_raise(self, exception: Exception):
        if not self.present():
            raise exception

        return self.data

    def __str__(self):
        return str(self.data)


EMPTY = Optional(None)


class StreamClosedException(Exception):
    """
    Exception thrown in case Stream is closed and stream functions
    are invoked.
    """
    pass


def _check_closed(is_closed: bool):
    """
    throws exception depending on is_closed
    :param is_closed:
    """
    if is_closed:
        raise StreamClosedException()


def _check_stream(func):
    """
    If Stream is closed then throws an exception otherwise,
    execute the function.
    :param func:
    :return:
    """

    @wraps(func)
    def f(self: 'Stream', *args, **kwargs):
        _check_closed(self.closed)
        return func(self, *args, **kwargs)

    return f


def _close_stream(func):
    """
    closes stream after executing the function.
    :param func:
    :return:
    """

    @wraps(func)
    def f(self: 'Stream', *args, **kwargs):
        out = func(self, *args, **kwargs)
        self.closed = True
        return out

    return f


class Stream(Generic[T]):
    def __init__(self, data: Iterable[T]):
        self._pointer = data
        self._close = False

    @property
    def closed(self) -> bool:
        """
        Checks if the stream has been operated with terminal operation
        such as 'count', min, max, group by, mapping, partition by etc.
        :return:
        """
        return self._close

    @closed.setter
    def closed(self, is_close: bool):
        """
        updates stream state.
        :param is_close:
        """
        self._close = is_close

    @_check_stream
    def map(self, func, worker=None, is_parallel=True) -> 'Stream[T]':
        """
        maps elements of stream.

        Exp:
        stream = Stream(range(5)).map(lambda x: 2*x)
        print(list(stream)) # prints [0, 2, 4, 6, 8]

        :param func:
        :param worker: number of worker to use in mapping element.
        :param is_parallel: works only if worker is not None and not equal to 1
        :return: Stream itself
        """

        if worker is not None and worker != 1:
            self._pointer = Stream.function_wrapper(func, worker, is_parallel)(self._pointer)
            func = Future.result

        self._pointer = map(func, self._pointer)
        return self

    @staticmethod
    def function_wrapper(func, worker: int, is_parallel: bool):
        """
        provides a wrapper around given function.
        :param func:
        :param worker:
        :param is_parallel:
        :return:
        """

        if is_parallel:
            Executor = futures.ProcessPoolExecutor
            if worker <= 0:
                worker = cpu_count()
                # in case, worker provided is less than 0, setting
                # number of cpu_core as default number of thread.
        else:
            Executor = futures.ThreadPoolExecutor
            if worker <= 0:
                worker = 4
                # in case, worker provided is less than 0, setting 4 as default
                # number of thread.

        @wraps(func)
        def f(generator: Iterable[T]):
            with Executor(max_workers=worker) as executor:
                for gs in divide_in_chunk(generator, worker):
                    yield from as_completed([executor.submit(func, g) for g in gs])

        return f

    @_check_stream
    def filter(self, predicate, worker=None, is_parallel=True) -> 'Stream[T]':
        """
        Filters elements from Stream.

        Exp:
        stream = Stream(range(5)).filter(lambda x: x%2 == 1)
        print(list(stream)) # prints [1, 3]

        :param predicate:
        :param worker: number of worker to use in filtration process.
        :param is_parallel: works only if worker is not None and not equal to 1
        :return: Stream itself
        """

        if worker is not None and worker != 1:
            self._pointer = Stream.function_wrapper(predicate, worker, is_parallel)(self._pointer)
            predicate = Future.result

        self._pointer = filter(predicate, self._pointer)
        return self

    @_check_stream
    def sorted(self, comp=None) -> 'Stream[T]':
        """
        Sorts element of Stream.
        stream = Stream([3,1,4,6]).sorted()
        list(stream) -> [1, 3, 4, 6]
        :param comp:
        :return: Stream itself
        """
        self._pointer = sorted(self._pointer, key=comp)
        return self

    @_check_stream
    def distinct(self) -> 'Stream[T]':
        """
        uses distinct element of for further processing.

        stream = Stream([4,1,6,1])
        list(stream) -> [1, 4, 6]

        Note that, sorting is not guaranteed. Elements must be hashable.

        :return:  Stream itself
        """
        self._pointer = set(self._pointer)
        return self

    @_check_stream
    def limit(self, n) -> 'Stream[T]':
        """
        limits number of element in stream
        Ex:
        stream = Stream(range(10)).limit(3)
        list(stream) -> [0,1,2]

        :param n:
        :return:
        """
        self._pointer = islice(self._pointer, n)
        return self

    @_check_stream
    def peek(self, consumer) -> 'Stream[T]':
        """
        processes element while streaming.

        Ex:
        def f(x):
            return 2*x
        stream = Stream(range(5)).peek(print).map(f)
        list(stream) first prints 0 to 4 and then make a list of [0, 2, 4, 6, 8]

        :param consumer:
        :return: Stream itself
        """
        self._pointer = Stream.consumer_wrapper(consumer)(self._pointer)
        # self._pointer = list(self._pointer)
        # print(self._pointer.__len__())
        return self

    @staticmethod
    def consumer_wrapper(consumer):
        """
        Creates a wrapper around consumer.
        :param consumer:
        :return:
        """

        @wraps(consumer)
        def func(generator: Iterable[T]) -> Iterable[T]:
            for g in generator:
                consumer(g)
                yield g

        return func

    @_check_stream
    def skip(self, n) -> 'Stream[T]':
        """
        Skips n number of element from Stream

        Ex:
        stream = Stream(range(10)).skip(7)
        list(stream) -> [7, 8, 9]
        :param n:
        :return:  Stream itself
        """
        self._pointer = islice(self._pointer, n, None)
        return self

    @_check_stream
    def flat_map(self) -> 'Stream[T]':
        """
        flats the stream if each element is iterable

        Ex:
        stream = Stream([[1,2],[3,4,5]])
        list(stream) -> [1, 2, 3, 4, 5]

        :return:Stream itself
        """
        self._pointer = chain.from_iterable(self._pointer)
        return self

    @_check_stream
    @_close_stream
    def partition(self, mapper=bool) -> Dict[bool, Sequence[T]]:
        """
        This operation closes the stream.

        Divides elements depending on mapper.

        Ex:
        stream = Stream(range(6))
        stream.partition(mapper=lambda x: x%2) -> {0:[0, 2, 4], 1:[1, 3, 5]}

        :param mapper:
        :return: Stream itself
        """
        return self.group_by(mapper)

    @_check_stream
    @_close_stream
    def count(self) -> int:
        """
        This operation closes the stream.

        :return: number of elements in Stream
        """
        return sum(1 for _ in self._pointer)

    @_check_stream
    @_close_stream
    def min(self, comp=None) -> Optional[Any]:
        """
        This operation closes the Stream.
        finds minimum element of Stream

        Ex:
        stream = Stream([3,1,5])
        item = stream.min().get() -> 1

        :param comp:
        :return:
        """
        try:
            return min(self._pointer, key=comp) if comp else min(self._pointer)
        except ValueError:
            return EMPTY

    @_check_stream
    @_close_stream
    def max(self, comp=None) -> Optional[Any]:
        """
        This operation closes the Stream.
        finds maximum element of Stream

        Ex:
        stream = Stream([3,1,5])
        item = stream.max().get() -> 5

        :param comp:
        :return:
        """
        try:
            return Optional(max(self._pointer, key=comp) if comp else max(self._pointer))
        except ValueError:
            return EMPTY

    @_check_stream
    @_close_stream
    def group_by(self, key_hasher, value_mapper=identity) -> Dict[Any, Sequence[T]]:
        """
        This operation closes the Stream.
        group by stream element using key_hasher.

        Ex1:
        stream  = Stream(range(10))
        stream.group_by(key_hasher=lambda x: x%3) -> {0:[0, 3, 6, 9], 1:[1, 4, 7], 2:{2, 5, 8}}

        Ex2:
        stream  = Stream(range(10))
        stream.group_by(key_hasher=lambda x: x%3,value_mapper=lambda x: x**2
        ) -> {0:[0, 9, 36, 81], 1:[1, 16, 49], 2:{4, 25, 64}}

        :param key_hasher:
        :param value_mapper:
        :return:
        """
        out = {}

        for elem in self._pointer:
            Stream._update(out, key_hasher(elem), value_mapper(elem))

        return out

    @_check_stream
    @_close_stream
    def mapping(self, key_mapper, value_mapper=identity) -> dict:
        """
        This operation closes the Stream.
        creates mapping from stream element.

        class Student:
            def __init__(self, name, id):
                self.name = name
                self.id  = id

        students = Stream([Student('a',1),Student('b',2),Student('a', 3)])
        students.mapping(key_mapper=lambda x:x.id, value_mapper=lambda x:x.name)
        -> {1: 'a', 2:'b', 3:'c'}

        :param key_mapper:
        :param value_mapper:
        :return:
        """
        out = {}

        for elem in self._pointer:
            k = key_mapper(elem)

            if k in out:
                raise ValueError('key {} is already present in map'.format(k))

            out[k] = value_mapper(elem)

        return out

    @staticmethod
    def _update(d: dict, k, v):
        if k not in d:
            pt = []
            d[k] = pt
        else:
            pt = d[k]

        pt.append(v)

    @_check_stream
    @_close_stream
    def __iter__(self) -> Iterable[T]:
        """
        This operation closes the Stream.
        :return:iterator from stream
        """
        return iter(self._pointer)

    @_check_stream
    @_close_stream
    def as_seq(self, seq_clazz=list) -> Sequence[T]:
        """
        This operation closes the Stream.
        returns Stream elements as sequence, for example as list.
        :param seq_clazz:
        :return:
        """
        return seq_clazz(self._pointer)

    @_check_stream
    @_close_stream
    def all(self, predicate=identity) -> bool:
        """
        This operation closes the Stream.
        returns True if all elements returns True. If there are no element in stream,
        returns True.

        Ex:
        class Student:
            def __init__(self, name, age):
                self.name = name
                self.age  = age

        stream = Stream([Student('a',10), Student('b',12)])
        stream.all(predicate=lambda x:x.age < 15) -> True

        :param predicate:
        :return:
        """
        return all(map(predicate, self._pointer))

    @_check_stream
    @_close_stream
    def any(self, predicate=identity) -> bool:
        """
        This operation closes the Stream.
        Returns True if atleast one element are True according to given predicate.
        Consequently, empty Stream returns False.

        Ex:
        class Student:
            def __init__(self, name, age):
                self.name = name
                self.age  = age

        stream = Stream([Student('a',10), Student('b',12)])
        stream.any(predicate=lambda x:x.age > 15) -> False

        :param predicate:
        :return:
        """
        return any(map(predicate, self._pointer))

    @_check_stream
    @_close_stream
    def none_match(self, predicate=identity) -> bool:
        """
        This operation closes the Stream.
        returns True if no element are true according to predicate.
        Empty stream returns True.
        Ex:
        class Student:
            def __init__(self, name, age):
                self.name = name
                self.age  = age

        stream = Stream([Student('a',10), Student('b',12)])

        stream.none_match(predicate=lambda x:x.age > 11) -> False

        Ex2:

        stream = Stream([Student('a',10), Student('b',12)])

        stream.none_match(predicate=lambda x:x.age > 13) -> True

        :param predicate:
        :return:
        """
        return not self.any(predicate)

    @_check_stream
    @_close_stream
    def find_first(self) -> Optional[Any]:
        """
        This operation closes the Stream.
        finds first element from Stream.
        :return:
        """
        for g in self._pointer:
            return Optional(g)

        return EMPTY

    @_check_stream
    @_close_stream
    def for_each(self, consumer):
        """
        This operation closes the Stream.
        consumes each element from stream.
        stream = Stream(range(5))
        stream.for_each(print)
        prints ->
        1
        2
        3
        4
        5

        :param consumer:
        """
        for g in self._pointer:
            consumer(g)

    @_check_stream
    @_close_stream
    def reduce(self, initial_point: T, bi_func) -> T:
        """
        This operation closes the Stream.
        reduces stream element to produce an element.

        stream = Stream(range(1,6))
        stream.reduce(1, lambda x,y: x*y) -> 120 (5!)

        :param initial_point:
        :param bi_func:
        :return:
        """
        for g in self._pointer:
            initial_point = bi_func(initial_point, g)

        return initial_point


if __name__ == 'utility.stream':
    __all__ = get_functions_clazz(__name__, __file__)
