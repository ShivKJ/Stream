from functools import wraps
from itertools import islice, chain
from typing import Iterable, TypeVar, Generic, Sequence, Dict, Any

from utility.utils import get_functions_clazz, identity

T = TypeVar('T')


class StreamClosedException(Exception):
    pass


def _check_closed(is_closed: bool):
    if is_closed:
        raise StreamClosedException()


def _check_stream(func):
    @wraps(func)
    def f(self: 'Stream', *args, **kwargs):
        _check_closed(self.closed)
        return func(self, *args, **kwargs)

    return f


def _close_stream(func):
    @wraps(func)
    def f(self: 'Stream', *args, **kwargs):
        out = func(self, *args, **kwargs)
        self.closed = True
        return out

    return f


class Optional(Generic[T]):
    def __init__(self, data: T):
        self.data = data

    def present(self):
        return self.data is not None

    def get(self):
        return self.data


EMPTY = Optional(None)


class Stream(Generic[T]):

    def __init__(self, data: Iterable[T]):
        self._pointer = data
        self._close = False

    @property
    def closed(self) -> bool:
        return self._close

    @closed.setter
    def closed(self, is_close: bool):
        self._close = is_close

    @_check_stream
    def map(self, func) -> 'Stream[T]':
        self._pointer = map(func, self._pointer)
        return self

    @_check_stream
    def filter(self, func) -> 'Stream[T]':
        self._pointer = filter(func, self._pointer)
        return self

    @_check_stream
    def sorted(self, comp=None) -> 'Stream[T]':
        self._pointer = sorted(self._pointer, key=comp)
        return self

    @_check_stream
    def limit(self, n) -> 'Stream[T]':
        self._pointer = islice(self._pointer, n)
        return self

    @_check_stream
    def peek(self, consumer) -> 'Stream[T]':
        self._pointer = Stream.consumer_wrapper(consumer)(self._pointer)
        return self

    @staticmethod
    def consumer_wrapper(consumer):

        @wraps(consumer)
        def func(generator: Iterable[T]) -> Iterable[T]:
            for g in generator:
                consumer(g)
                yield g

        return func

    @_check_stream
    def skip(self, n) -> 'Stream[T]':
        self._pointer = islice(self._pointer, n, None)
        return self

    @_check_stream
    def flat_map(self) -> 'Stream[T]':
        self._pointer = chain.from_iterable(self._pointer)
        return self

    @_check_stream
    @_close_stream
    def partition(self, mapper=bool) -> Dict[bool, Sequence[T]]:
        return self.group_by(mapper)

    @_check_stream
    @_close_stream
    def count(self) -> int:
        """
        :return: number of elements in Stream
        """
        return sum(1 for _ in self._pointer)

    @_check_stream
    @_close_stream
    def min(self, comp=None) -> Optional[Any]:
        try:
            return min(self._pointer, key=comp) if comp else min(self._pointer)
        except ValueError:
            return EMPTY

    @_check_stream
    @_close_stream
    def max(self, comp=None) -> Optional[Any]:
        try:
            return Optional(max(self._pointer, key=comp) if comp else max(self._pointer))
        except ValueError:
            return EMPTY

    @_check_stream
    @_close_stream
    def group_by(self, key_hasher, value_mapper=identity) -> Dict[Any, Sequence[T]]:
        out = {}

        for elem in self._pointer:
            Stream._update(out, key_hasher(elem), value_mapper(elem))

        return out

    @_check_stream
    @_close_stream
    def mapping(self, key_mapper, value_mapper=identity) -> dict:
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
        return iter(self._pointer)

    @_check_stream
    @_close_stream
    def as_seq(self, seq_clazz=list) -> Sequence[T]:
        return seq_clazz(self._pointer)

    @_check_stream
    @_close_stream
    def all(self, predicate=identity) -> bool:
        return all(map(predicate, self._pointer))

    @_check_stream
    @_close_stream
    def any(self, predicate=identity) -> bool:
        return any(map(predicate, self._pointer))

    @_check_stream
    @_close_stream
    def none_match(self, predicate=identity) -> bool:
        return not self.any(predicate)

    @_check_stream
    @_close_stream
    def find_first(self) -> Optional[Any]:
        for g in self._pointer:
            return Optional(g)

        return EMPTY

    @_check_stream
    @_close_stream
    def for_each(self, consumer):
        for g in self._pointer:
            consumer(g)

    @_check_stream
    @_close_stream
    def reduce(self, initial_point: T, bi_func) -> T:

        for g in self._pointer:
            initial_point = bi_func(initial_point, g)

        return initial_point


if __name__ == 'utility.stream':
    __all__ = get_functions_clazz(__name__, __file__)
