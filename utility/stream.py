from functools import wraps
from itertools import islice
from typing import Iterable, TypeVar, Generic, Sequence, Dict, Any

from utility.utils import get_functions_clazz

T = TypeVar('T')


class StreamClosedException(Exception):
    pass


def _check_closed(is_closed: bool):
    if is_closed:
        raise StreamClosedException()


def _check_stream(func):
    @wraps(func)
    def f(self, *args, **kwargs):
        _check_closed(self._close)
        print(func.__name__, self._close)

        return func(self, *args, **kwargs)

    return f


class Stream(Generic[T]):

    def __init__(self, data: Iterable[T]):
        self.pointer = data
        self._close = False

    def close(self):
        self._close = True

    @_check_stream
    def map(self, func) -> 'Stream[T]':
        self.pointer = map(func, self.pointer)
        return self

    @_check_stream
    def filter(self, func) -> 'Stream[T]':
        self.pointer = filter(func, self.pointer)
        return self

    @_check_stream
    def count(self) -> int:

        count = sum(1 for _ in self.pointer)
        self.close()

        return count

    @_check_stream
    def min(self, comp=None) -> T:
        self.close()
        return min(self.pointer, key=comp)

    @_check_stream
    def max(self, comp=None) -> T:
        self.close()
        return max(self.pointer, key=comp)

    @_check_stream
    def sort(self, comp=None) -> 'Stream[T]':
        self.pointer = sorted(self.pointer, key=comp)
        return self

    @_check_stream
    def limit(self, n) -> 'Stream[T]':
        self.pointer = islice(self.pointer, n)
        return self

    @_check_stream
    def groupBy(self, hasher) -> Dict[Any, Sequence[T]]:
        out = {}

        for elem in self.pointer:
            Stream._update(out, hasher(elem), elem)

        self.close()

        return out

    @_check_stream
    def mapping(self, key_mapper, value_mapper) -> dict:
        out = {}

        for elem in self.pointer:
            k = key_mapper(elem)

            if out:
                raise ValueError('key {} is already present in map'.format(k))

            out[k] = value_mapper(elem)

        self.close()

        return out

    @_check_stream
    def partition(self, mapper=bool) -> Dict[bool, Sequence[T]]:
        return self.groupBy(mapper)

    @staticmethod
    def _update(d: dict, k, v):
        if k not in d:
            d[k] = []

        d[k].append(v)

    @_check_stream
    def __iter__(self) -> Iterable[T]:
        self.pointer = iter(self.pointer)

        for elem in self.pointer:
            yield elem

        self.close()


if __name__ == 'utility.stream':
    __all__ = get_functions_clazz(__name__, __file__)
