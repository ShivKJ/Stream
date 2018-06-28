from abc import abstractmethod
from functools import wraps
from itertools import accumulate, chain, islice
from typing import Any, Dict, Generic, Iterable, Sequence, Union

from streamAPI.stream.decos import check_stream, close_stream
from streamAPI.stream.optional import EMPTY, Optional
from streamAPI.utility.Types import BiFunction, Callable, Consumer, Function, T, X, Y, Z
from streamAPI.utility.utils import (Filter, divide_in_chunk, get_functions_clazz,
                                     identity)

NIL = object()


class GroupByValueType(type):
    def __init__(cls, *args, **kwargs):
        type.__init__(cls, *args, **kwargs)

    @abstractmethod
    def add(self, o):
        pass


class ListType(list, metaclass=GroupByValueType):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def add(self, o):
        return self.append(o)


class SetType(set, metaclass=GroupByValueType):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    add = set.add


class Stream(Generic[X]):

    def __init__(self, data: Iterable[X]):
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

    @check_stream
    def map(self, func: Function[X, Y]) -> 'Stream[Y]':
        """
        maps elements of stream and produces stream of mapped element.

        Example:
            stream = Stream(range(5)).map(lambda x: 2*x)
            print(list(stream)) # prints [0, 2, 4, 6, 8]

        :param func:
        :return: Stream itself
        """

        self._pointer = map(func, self._pointer)
        return self

    @check_stream
    def filter(self, predicate: Filter[X]) -> 'Stream[X]':
        """
        Filters elements from Stream.

        Example:
            stream = Stream(range(5)).filter(lambda x: x%2 == 1)
            print(list(stream)) # prints [1, 3]

        :param predicate:
        :return: Stream itself
        """

        self._pointer = filter(predicate, self._pointer)
        return self

    @check_stream
    def sorted(self, comp=None, reverse: bool = False) -> 'Stream[X]':
        """
        Sorts element of Stream.

        Example1:
            stream = Stream([3,1,4,6]).sorted()
            list(stream) -> [1, 3, 4, 6]

            Stream([3,1,4,6]).sorted(reverse=True).as_seq() -> [6, 4, 3, 1]

        Example2:
            class Student:
                def __init__(self, name, age):
                    self.name = name
                    self.age = age

                def get_age(self):
                    return self.age

                def __str__(self):
                    return '[name='+self.name+',age='+str(self.age)+']'

                def __repr__(self):
                    return str(self)

            students = [Student('A',3),Student('B',1),Student('C',4),Student('D',6)]

            Stream(students).sorted(comp=Student.get_age,reverse=True).as_seq()
            -> [[name=D,age=6], [name=C,age=4], [name=A,age=3], [name=B,age=1]]

        :param comp:
        :param reverse:
        :return: Stream itself
        """

        self._pointer = sorted(self._pointer, key=comp, reverse=reverse)
        return self

    @check_stream
    def distinct(self) -> 'Stream[X]':
        """
        uses distinct element of for further processing.

        Example:
            stream = Stream([4,1,6,1]).distinct()
            list(stream) -> [1, 4, 6]

        Note that, sorting is not guaranteed.
        Elements must be hashable and define equal logic(__eq__)

        :return:  Stream itself
        """

        self._pointer = set(self._pointer)
        return self

    @check_stream
    def limit(self, n: int) -> 'Stream[X]':
        """
        limits number of element in stream.

        Example:
            stream = Stream(range(10)).limit(3)
            list(stream) -> [0,1,2]

        :param n:
        :return:
        """

        self._pointer = islice(self._pointer, n)
        return self

    @check_stream
    def peek(self, consumer: Consumer[X]) -> 'Stream[X]':
        """
        processes element while streaming.

        Example:
            def f(x): return 2*x

            stream = Stream(range(5)).peek(print).map(f)
            list(stream) first prints 0 to 4 and then makes a list of [0, 2, 4, 6, 8]

        :param consumer:
        :return: Stream itself
        """

        self._pointer = Stream._consumer_wrapper(consumer)(self._pointer)
        return self

    @staticmethod
    def _consumer_wrapper(consumer: Consumer[X]):
        """
        Creates a wrapper around consumer.

        :param consumer:
        :return:
        """

        @wraps(consumer)
        def func(generator: Iterable[X]) -> Iterable[X]:
            for g in generator:
                consumer(g)
                yield g

        return func

    @check_stream
    def skip(self, n: int) -> 'Stream[X]':
        """
        Skips n number of element from Stream

        Example:
            stream = Stream(range(10)).skip(7)
            list(stream) -> [7, 8, 9]

        :param n:
        :return:  Stream itself
        """

        self._pointer = islice(self._pointer, n, None)
        return self

    @check_stream
    def flat_map(self) -> 'Stream[X]':
        """
        flats the stream if each element is iterable.

        Example:
            stream = Stream([[1,2],[3,4,5]]).flat_map()
            list(stream) -> [1, 2, 3, 4, 5]

        :return:Stream itself
        """

        self._pointer = chain.from_iterable(self._pointer)
        return self

    @check_stream
    def batch(self, n: int):
        """
        creates batches of size n from stream.

        Example:
            Stream(range(10)).batch(3).as_seq()
            -> [(0, 1, 2), (3, 4, 5), (6, 7, 8), (9,)]

        :param n: batch size
        :return:
        """

        self._pointer = divide_in_chunk(self._pointer, n)

        return self

    @check_stream
    def enumerate(self):
        """
        create stream of tuples where first entry is index and
        another is data itself.

        Example:
            Stream(range(4,10)).enumerate().as_seq()
            -> [(0, 4), (1, 5), (2, 6), (3, 7), (4, 8), (5, 9)]

        :return:
        """

        self._pointer = enumerate(self._pointer)

        return self

    @check_stream
    @close_stream
    def partition(self, mapper: Filter[X] = bool) -> Dict[bool, Sequence[X]]:
        """
        This operation is one of the terminal operations
        partition elements depending on mapper.

        Example:
            stream = Stream(range(6))
            stream.partition(mapper=lambda x: x%2) -> {0:[0, 2, 4], 1:[1, 3, 5]}

        :param mapper:
        :return: Stream itself
        """

        return self.group_by(mapper)

    @check_stream
    @close_stream
    def count(self) -> int:
        """
        This operation is one of the terminal operations

        Example:
            Stream(range(10)).count() -> 10

        :return: number of elements in Stream
        """

        return sum(1 for _ in self._pointer)

    @check_stream
    @close_stream
    def min(self, comp=None) -> Optional[Any]:
        """
        This operation is one of the terminal operations
        finds minimum element of Stream

        Example1:
            stream = Stream([3,1,5])
            item = stream.min().get() -> 1

        Example2:
            class Student:
                def __init__(self, name, age):
                    self.name = name
                    self.age = age

                def get_age(self):
                    return self.age

                def __str__(self):
                    return '[name='+self.name+',age='+str(self.age)+']'

                def __repr__(self):
                    return str(self)

            students = [Student('A',3),Student('B',1),Student('C',4),Student('D',6)]

            Stream(students).min(comp=Student.get_age) -> Optional[[name=B,age=1]]

        :param comp:
        :return:
        """

        try:
            return Optional(min(self._pointer, key=comp) if comp else min(self._pointer))
        except ValueError:
            return EMPTY

    @check_stream
    @close_stream
    def max(self, comp=None) -> Optional[Any]:
        """
        This operation is one of the terminal operations.
        finds maximum element of Stream

        Example1:
            stream = Stream([3,1,5])
            item = stream.max().get() -> 5

        Example2:
            class Student:
                def __init__(self, name, age):
                    self.name = name
                    self.age = age

                def get_age(self):
                    return self.age

                def __str__(self):
                    return '[name='+self.name+',age='+str(self.age)+']'

                def __repr__(self):
                    return str(self)

            students = [Student('A',3),Student('B',1),Student('C',4),Student('D',6)]

            Stream(students).max(comp=Student.get_age) -> Optional[[name=D,age=6]]

        :param comp:
        :return:
        """

        try:
            return Optional(max(self._pointer, key=comp) if comp else max(self._pointer))
        except ValueError:
            return EMPTY

    @check_stream
    @close_stream
    def group_by(self, key_hasher, value_mapper: Function[X, Y] = identity,
                 value_container_clazz: GroupByValueType = ListType) -> Dict[Any, Sequence[Y]]:
        """
        This operation is one of the terminal operations
        group by stream element using key_hasher.

        Example1:
            stream  = Stream(range(10))
            stream.group_by(key_hasher=lambda x: x%3) -> {0:[0, 3, 6, 9], 1:[1, 4, 7], 2:[2, 5, 8]}

        Example2:
            stream  = Stream(range(10))
            stream.group_by(key_hasher=lambda x: x%3,value_mapper=lambda x: x**2)
            -> {0:[0, 9, 36, 81], 1:[1, 16, 49], 2:[4, 25, 64]}

        Example3:
            out = Stream([1, 2, 3, 4, 2, 4]).group_by(lambda x:x%2,value_container_type=ListType)
            -> {1: [1, 3], 0: [2, 4, 2, 4]}

            out =Stream([1, 2, 3, 4, 2, 4]).group_by(lambda x:x%2,value_container_type=SetType)
            -> {1: {1, 3}, 0: {2, 4}}

        :param key_hasher:
        :param value_mapper:
        :param value_container_clazz:
        :return:
        """

        out = {}

        for elem in self._pointer:
            Stream._update(out, key_hasher(elem), value_mapper(elem), value_container_clazz)

        return out

    @staticmethod
    def _update(d: dict, k, v: X, value_container_clazz: GroupByValueType):
        if k not in d:
            pt = value_container_clazz()
            d[k] = pt
        else:
            pt = d[k]

        pt.add(v)

    @check_stream
    @close_stream
    def mapping(self, key_mapper: Function[X, T],
                value_mapper: Function[X, Y] = identity,
                resolve: BiFunction[Y, Y, Z] = None) -> Dict[T, Union[Y, Z]]:
        """
        This operation is one of the terminal operations
        creates mapping from stream element.

        Example:
            class Student:
                def __init__(self, name, id):
                    self.name = name
                    self.id  = id

            students = Stream([Student('a',1),Student('b',2),Student('a', 3)])
            students.mapping(key_mapper=lambda x:x.id, value_mapper=lambda x:x.name)
            -> {1: 'a', 2:'b', 3:'c'}
        Notice that elements after operated upon by function "key_mapper" must be
        unique. In case of duplicity, ValueError is thrown.

        for example:
            out = Stream([1,2,1,3]).mapping(lambda x:x, lambda x:x**2)

        will throw ValueError as a value (1) is present multiple times.

        In case we need to resolve such issues we can pass a function which
        will take oldValue and newValue and return a value which will be set
        for the key.

        out = Stream([1,2,3,4,5,6]).mapping(lambda x: x%2 ,lambda x:x, lambda o,n : o + n)
        print (out) # prints {0:12, 1: 9}


        :param key_mapper:
        :param value_mapper:
        :param resolve
        :return:
        """

        out = {}

        for elem in self._pointer:
            k = key_mapper(elem)

            if k in out:
                if resolve is None:
                    raise ValueError('key {} is already present in map'.format(k))
                out[k] = resolve(out[k], value_mapper(elem))
            else:
                out[k] = value_mapper(elem)

        return out

    @check_stream
    @close_stream
    def as_seq(self, seq_clazz: Callable[[Iterable[X], None], Y] = list, **kwargs) -> Y:
        """
        This operation is one of the terminal operations
        returns Stream elements as sequence, for example as list.

        Example:
            Stream(range(5)).as_seq() -> [1, 2, 3, 4, 5]

            from numpy import fromiter
            Stream(range(5)).as_seq(fromiter, dtype=int) -> array([0, 1, 2, 3, 4])

        :param seq_clazz:
        :param kwargs
        :return:
        """

        return seq_clazz(self._pointer, **kwargs)

    @check_stream
    @close_stream
    def all(self, predicate: Filter[X] = identity) -> bool:
        """
        This operation is one of the terminal operations
        returns True if all elements returns True. If there are no element in stream,
        returns True.

        Example:
            class Student:
                def __init__(self, name, age):
                    self.name = name
                    self.age  = age

            stream = Stream([Student('a',10), Student('b',12)])
            stream.all(predicate=lambda x:x.age < 15) -> True

            Stream([]).all() -> True # Empty Stream returns True
            Stream([0]).all() -> False
            Stream([1]).all() -> True

        :param predicate:
        :return:
        """

        return all(map(predicate, self._pointer))

    @check_stream
    @close_stream
    def any(self, predicate: Filter[X] = identity) -> bool:
        """
        This operation is one of the terminal operations
        Returns True if at-least one element are True according to given predicate.
        Consequently, empty Stream returns False.

        Example:
            class Student:
                def __init__(self, name, age):
                    self.name = name
                    self.age  = age

            stream = Stream([Student('a',10), Student('b',12)])
            stream.any(predicate=lambda x:x.age > 15) -> False

            Stream([]).any() -> False
            Stream([1]).any() -> True
            Stream([0]).any() -> False

        :param predicate:
        :return:
        """

        return any(map(predicate, self._pointer))

    @check_stream
    @close_stream
    def none_match(self, predicate: Filter[X] = identity) -> bool:
        """
        This operation is one of the terminal operations
        returns True if no element are true according to predicate.
        Empty stream returns True.
        Example:
            class Student:
                def __init__(self, name, age):
                    self.name = name
                    self.age  = age

            stream = Stream([Student('a',10), Student('b',12)])
            stream.none_match(predicate=lambda x:x.age > 11) -> False

        Example:
            stream = Stream([Student('a',10), Student('b',12)])
            stream.none_match(predicate=lambda x:x.age > 13) -> True

        Example:
            Stream([]).none_match(lambda x: x == 5) -> True
            Stream([1]).none_match(lambda x: x == 5) -> True
            Stream([1,5]).none_match(lambda x: x == 5) -> False

        :param predicate:
        :return:
        """
        return not self.any(predicate)

    @check_stream
    @close_stream
    def find_first(self) -> Optional[Any]:
        """
        This operation is one of the terminal operations
        finds first element from Stream.

        Example:
            Stream(range(4,9)).find_first() -> Optional[4]

        :return:
        """

        for g in self._pointer:
            return Optional(g)

        return EMPTY

    @check_stream
    @close_stream
    def for_each(self, consumer: Consumer[X]):
        """
        This operation is one of the terminal operations
        consumes each element from stream.
        Example:
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

    @check_stream
    @close_stream
    def reduce(self, bi_func: BiFunction[X, X, Y], initial_point: X = NIL) -> Optional[Y]:
        """
        This operation is one of the terminal operations
        reduces stream element to produce an element.

        stream = Stream(range(1,6))
        stream.reduce(lambda x,y: x*y, 1) -> 120 (5!)

        Case Without initial point(initial__pointer is NIL):
            Return value can only be EMPTY iff Stream does not having
            any element left in it.

            SUMMING = lambda x,y : x + y

            Stream([]).reduce(SUMMING) -> EMPTY
            Stream([1]).reduce(SUMMING) -> Optional[1]
            Stream([1, 2]).reduce(SUMMING) -> Optional[3]

        Case With Initial Point (initial_point is not NIL):
            Return value will never be EMPTY.

            SUMMING = lambda x,y : x + y
            initial_point = 10

            Stream([]).reduce(SUMMING, initial_point) -> Optional[10]
            Stream([1]).reduce(SUMMING, initial_point) -> Optional[11]
            Stream([1, 2]).reduce(SUMMING, initial_point) -> Optional[13]

        :param initial_point:
        :param bi_func:
        :return:
        """

        if initial_point is not NIL:
            self._pointer = chain((initial_point,), self._pointer)

        self._pointer = accumulate(self._pointer, bi_func)

        for initial_point in self._pointer: pass

        return EMPTY if initial_point is NIL else Optional(initial_point)

    @check_stream
    @close_stream
    def __iter__(self) -> Iterable[X]:
        """
        This operation is one of the terminal operations

        Example:
            for i in Stream(range(5)):
                print(i)

            prints: 1\n2\n3\n4\n5
        :return:iterator from stream
        """

        return iter(self._pointer)


if __name__ == 'streamAPI.stream.stream':
    __all__ = get_functions_clazz(__name__, __file__)
