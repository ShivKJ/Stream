"""
author: Shiv
email: shivkj001@gmail.com
"""

from functools import reduce, wraps
from itertools import (accumulate, chain, cycle, dropwhile, filterfalse, islice, takewhile,
                       zip_longest)
from typing import Any, Generic, Iterable, Tuple, Union

from streamAPI.stream.TO.TerminalOperations import Collector
from streamAPI.stream.decos import check_pipeline, close_pipeline
from streamAPI.stream.optional import EMPTY, Optional
from streamAPI.stream.streamHelper import ChainedCondition, Closable, Supplier
from streamAPI.utility.Types import BiFunction, Callable, Consumer, Filter, Function, X, Y
from streamAPI.utility.utils import NIL, divide_in_chunk, get_chunk, get_functions_clazz, identity


class Stream(Closable, Generic[X]):
    """
    This class can be used to create pipeline operation on given
    iterable object.

    There are two type of operation that can be performed on Stream:
    1) Intermediate (i.e. map, flat_map, filter, peek, distinct, sort, batch, cycle, take_while, drop_while etc.)
    2) Terminal (min, max, as_seq, group_by, all, any, none_match, for_each etc.)

    Until terminal operation is called, no execution takes place.
    Some of the examples are given below.
    
    Example:
        from streamAPI.stream import *
        from streamAPI.stream.TO import *
        from operator import attrgetter,itemgetter

        class Student:
            def __init__(self, name, age, sex):
                self.name = name
                self.age = age
                self.sex = sex

            def get_age(self):
                return self.age

            def __str__(self):
                return '[name='+self.name+',age='+str(self.age)+',sex='+('Male' if self.sex else 'Female')+']'

            def __repr__(self):
                return str(self)

        students = [Student('A',10,True),
                    Student('B',8,True),
                    Student('C',11,False),
                    Student('D',17,True),
                    Student('D',25,False),
                    Student('F',9,False),
                    Student('G',29,True)]

        Stream(students).filter(lambda x:x.age<20).sort(lambda x:x.age).collect(ToList())

        -> [[name=B,age=8,sex=Male],
            [name=F,age=9,sex=Female],
            [name=A,age=10,sex=Male],
            [name=C,age=11,sex=Female],
            [name=D,age=17,sex=Male]]


        Stream(students).filter(lambda x:x.age<20).collect(GroupingBy(attrgetter('sex')))

        -> {True: [[name=A,age=10,sex=Male],
                   [name=B,age=8,sex=Male],
                   [name=D,age=17,sex=Male]],
            False: [[name=C,age=11,sex=Female],
                    [name=F,age=9,sex=Female]]}

        Stream(range(10)).map(lambda x: x**3 - x**2).filter(lambda x: x%3 == 0).skip(3).collect(ToList())
        ->  [48, 180, 294, 648]

        Stream(range(10)).batch(3).collect(ToList()) -> [(0, 1, 2), (3, 4, 5), (6, 7, 8), (9,)]

        Stream([(0, 1, 2), (3, 4, 5), (6, 7, 8), (9,)]).flat_map().limit(6).collect(ToList())
        -> [0, 1, 2, 3, 4, 5]

        Stream(range(10)).take_while(lambda x:x<5).collect(ToList()) # similar to while loop
        -> [0, 1, 2, 3, 4]

        Stream(range(10)).drop_while(lambda x:x<5).collect(ToList())
        -> [5, 6, 7, 8, 9]

        Stream(range(3,9)).zip(range(4)).collect(ToList()) -> [(3, 0), (4, 1), (5, 2), (6, 3)]

        Stream(range(3,9)).zip(range(4),after=False).collect(ToList()) -> [(0, 3), (1, 4), (2, 5), (3, 6)]

        Stream(range(3,9)).zip_longest(range(4),fillvalue=-1).collect(ToList())
        -> [(3, 0), (4, 1), (5, 2), (6, 3), (7, -1), (8, -1)]

        Stream(range(3,9)).zip_longest(range(4),after=False,fillvalue=-1).collect(ToList())
        -> [(0, 3), (1, 4), (2, 5), (3, 6), (-1, 7), (-1, 8)]

        Stream(range(10)).map(lambda x:x**2).reduce(bi_func=op.add) # sum of squares
        -> Optional[285]

        Stream(['AB', 'BD', 'AC', 'DE', 'BD', 'BW', 'AB']).collect(GroupingBy(itemgetter(0),
                                                                      GroupingBy(itemgetter(1),
                                                                                 Counting())))

        -> {
             "A": {
              "B": 2,
              "C": 1
             },
             "B": {
              "D": 2,
              "W": 1
             },
             "D": {
              "E": 1
             }
            }

    """

    def __init__(self, data: Iterable[X]):
        super().__init__()

        self._pointer = iter(data)

    @classmethod
    def from_supplier(cls, func: Callable[[], X], *args, **kwargs) -> 'Stream[X]':
        """

        Generates a stream from a callable function.(see Supplier class in
        streamHelper module for more detail).

        :param func:
        :param args: positional arguments required instantiate cls
        :param kwargs: kwargs required for cls.

        :return: a new Stream class object.
        """

        return cls(Supplier(func), *args, **kwargs)

    @check_pipeline
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

    @check_pipeline
    def filter(self, predicate: Filter[X]) -> 'Stream[X]':
        """
        Filters elements from Stream, i.e. if predicates evaluates an
        element as False, then the elements is not considered for
        further processing.

        Example:
            stream = Stream(range(5)).filter(lambda x: x%2 == 1)
            print(list(stream)) # prints [1, 3]

        :param predicate:
        :return: Stream itself
        """

        self._pointer = filter(predicate, self._pointer)
        return self

    @check_pipeline
    def exclude(self, predicate: Filter[X]) -> 'Stream[X]':
        """
        Excluding an element from the stream if 'predicate' returns True for
        it.

        from streamAPI.stream import *

        def is_odd(x): return x%2==1

        Stream(range(10)).exclude(is_odd).collect(TO.ToList())
        -> [0, 2, 4, 6, 8] # every odd number will be excluded.

        :param predicate:
        :return: Stream itself
        """

        self._pointer = filterfalse(predicate, self._pointer)
        return self

    @check_pipeline
    def sort(self, key=None, reverse: bool = False) -> 'Stream[X]':
        """
        Sorts element of Stream.

        Example1:
            stream = Stream([3,1,4,6]).sort()
            list(stream) -> [1, 3, 4, 6]

            Stream([3,1,4,6]).sort(reverse=True).collect(ToList()) -> [6, 4, 3, 1]

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

            Stream(students).sorted(key=Student.get_age,reverse=True).collect(ToList())
            -> [[name=D,age=6], [name=C,age=4], [name=A,age=3], [name=B,age=1]]

        :param key:
        :param reverse:
        :return: Stream itself
        """

        self._pointer = Stream._yield_sorted(self._pointer, key, reverse)
        return self

    @staticmethod
    def _yield_sorted(itr: Iterable[X], key, reverse: bool) -> Iterable[X]:
        """
        Creates a generator having elements in sorted order.

        :param itr:
        :param key:
        :param reverse:
        :return:
        """

        yield from sorted(itr, key=key, reverse=reverse)

    @check_pipeline
    def distinct(self) -> 'Stream[X]':
        """
        uses distinct element of for further processing.

        Example:
            stream = Stream([4,1,6,1]).distinct()
            list(stream) -> [1, 4, 6]

        Note that, sorting is not guaranteed.
        Elements must be hashable and define equal logic(__eq__)

        :return: Stream itself
        """

        self._pointer = Stream._yield_distinct(self._pointer)
        return self

    @staticmethod
    def _yield_distinct(itr: Iterable[X]):
        """
        yield distinct elements from a given iterable

        :param itr:
        :return: generator of distinct elements
        """

        consumer_items = set()

        for item in itr:
            if item not in consumer_items:
                yield item
                consumer_items.add(item)

    @check_pipeline
    def limit(self, n: int) -> 'Stream[X]':
        """
        limits number of elements for further processing.

        Example:
            stream = Stream(range(10)).limit(3)
            list(stream) -> [0,1,2]

        :param n: maximum number of elements to be considered.
        :return: Stream itself
        """

        self._pointer = islice(self._pointer, n)
        return self

    @check_pipeline
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

    @check_pipeline
    def peek_after_each(self, consumer: Consumer[X], n: int) -> 'Stream[X]':
        """
        processes element while streaming. Consumer is called after each nth item.

        Example:
            def f(x): return 2*x

            stream = Stream(range(10)).peek_after_each(print,3).map(f)
            list(stream) first prints "2\n5\n8" and then makes a list
            [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]

        :param consumer:
        :param n: invoking the consumer after each 'n'.
        :return: Stream itself
        """

        assert n > 0, 'n should be a natural number.'

        self._pointer = Stream._consumer_wrapper(consumer, n=n)(self._pointer)
        return self

    @staticmethod
    def _consumer_wrapper(consumer: Consumer[X], n: int = 1):
        """
        Creates a wrapper around consumer.

        :param consumer:
        :param n: a natural number
        :return: a decorator which enables iterable to be consumed after each 'n'
        """

        @wraps(consumer)
        def func(itr: Iterable[X]) -> Iterable[X]:
            one_to_n = cycle(range(1, n + 1))  # cycling numbers from 1 up to n

            for idx, g in zip(one_to_n, itr):
                if idx == n:
                    consumer(g)

                yield g

        return func

    @check_pipeline
    def skip(self, n: int) -> 'Stream[X]':
        """
        Skips "n" number of elements for further processing.

        Example:
            stream = Stream(range(10)).skip(7)
            list(stream) -> [7, 8, 9]

        :param n:
        :return: Stream itself
        """

        self._pointer = islice(self._pointer, n, None)
        return self

    @check_pipeline
    def flat_map(self) -> 'Stream[X]':
        """
        flats Stream of Iterable.

        Example:
            stream = Stream([[1,2],[3,4,5]]).flat_map()
            list(stream) -> [1, 2, 3, 4, 5]

        :return: Stream itself
        """

        self._pointer = chain.from_iterable(self._pointer)
        return self

    @check_pipeline
    def batch(self, n: int):
        """
        creates batches of size "n" for further processing.

        Example:
            Stream(range(10)).batch(3).collect(ToList())
            -> [(0, 1, 2), (3, 4, 5), (6, 7, 8), (9,)]

        :param n: batch size
        :return: Stream itself
        """

        self._pointer = divide_in_chunk(self._pointer, n)
        return self

    @check_pipeline
    def enumerate(self, start=0):
        """
        create stream of tuples where first entry is the index and
        another is stream element.

        Example:
            Stream(range(4,10)).enumerate().collect(ToList())
            -> [(0, 4), (1, 5), (2, 6), (3, 7), (4, 8), (5, 9)]

            Stream(range(4,10)).enumerate(10).collect(ToList())
             [(10, 4), (11, 5), (12, 6), (13, 7), (14, 8), (15, 9)]

        :param start
        :return: Stream itself
        """

        self._pointer = enumerate(self._pointer, start=start)
        return self

    @check_pipeline
    def take_while(self, predicate: Filter[X]) -> 'Stream[X]':
        """
        processes the element of stream till the predicate returns True.
        It is similar to "while" keyword.

        Stream(range(10)).till(lambda x:x < 5).collect(ToList()) -> [0,1,2,3,4]

        :param predicate:
        :return: Stream itself
        """

        self._pointer = takewhile(predicate, self._pointer)
        return self

    @check_pipeline
    def drop_while(self, predicate: Filter[X]) -> 'Stream[X]':
        """
        drops elements until predicate returns False.

        stream.Stream(range(10)).drop_while(lambda x : x < 5).collect(ToList()) -> [5, 6, 7, 8, 9]

        :param predicate:
        :return: Stream itself
        """

        self._pointer = dropwhile(predicate, self._pointer)
        return self

    @check_pipeline
    def zip(self, *itr: Iterable[Y], after=True) -> 'Stream[Tuple]':
        """
        zips stream with another Iterable object.

        We can specify whether to zip iterable after the stream of before
        the stream by using "after".

        zip operation will produce a stream which will be exhausted if either
        itr has been exhausted or underlying stream is exhausted.

        Example:
            Stream(range(100, 100000)).zip(range(5)).collect(ToList())
            -> [(100, 0), (101, 1), (102, 2), (103, 3), (104, 4)]

            Stream(range(5)).zip(range(100, 100000)).collect(ToList())
            -> [(0, 100), (1, 101), (2, 102), (3, 103), (4, 104)]

            Stream(range(20, 30)).zip(range(5),after=False).collect(ToList())
            # data from range(5) will be used as first entry of tuple created by zipping
            # stream with iterable
            -> [(0, 20), (1, 21), (2, 22), (3, 23), (4, 24)]

        :param itr:
        :param after
        :return: Stream itself
        """

        if after:
            self._pointer = zip(self._pointer, *itr)
        else:
            self._pointer = zip(*itr, self._pointer)

        return self

    @check_pipeline
    def zip_longest(self, *itr: Iterable[Y], after=True, fillvalue=None) -> 'Stream[Tuple]':
        """
        Unlike zip method which limits resultant stream depending on smaller iterable,
        zip_longest allow stream generator even though smaller iterable has been exhausted.
        default filling value will be used from "fillvalue".

        Example:
            Stream(range(11, 13)).zip_longest(range(5)).collect(ToList())
            -> [(11, 0), (12, 1), (None, 2), (None, 3), (None, 4)]

            Stream(range(11, 13)).zip_longest(range(5),after=False,fillvalue=-1).collect(ToList())
            -> [(0, 11), (1, 12), (2, -1), (3, -1), (4, -1)]

        :param itr:
        :param after:defaults to True
        :param fillvalue: defaults to None.
        :return: Stream itself
        """

        if after:
            self._pointer = zip_longest(self._pointer, *itr, fillvalue=fillvalue)
        else:
            self._pointer = zip_longest(*itr, self._pointer, fillvalue=fillvalue)

        return self

    @check_pipeline
    def cycle(self, itr: Iterable[Y], after=True) -> 'Stream[Tuple]':
        """
        Repeats iterable "itr" with stream until the Stream is exhausted.

        Example:
            Stream(range(11, 16)).cycle(range(3),after=False).collect(ToList())
            -> [(0, 11), (1, 12), (2, 13), (0, 14), (1, 15)]

        :param itr:
        :param after: defaults to True
        :return: Stream itself
        """

        return self.zip(cycle(itr), after=after)

    @check_pipeline
    def if_else(self, if_: Filter[X],
                then: Function[X, Y],
                else_: Function[X, Y] = identity) -> 'Stream[Y]':
        """
        if "if_" returns True then elements are transformed according to "then" otherwise else_
        function is used. This method is the special case of "conditional" method.
        "else_" has default value "identity" which return element as it is in case "if_" fails;

        Example:
            Stream(range(10)).condition(lambda x: 3 <= x <= 7, lambda x: 1 , lambda x: 0).collect(ToList())
            -> [0, 0, 0, 1, 1, 1, 1, 1, 0, 0]

        :param if_:
        :param then:
        :param else_: by default it returns element as it is.
        :return: Stream itself
        """

        return self.map(ChainedCondition.if_else(if_, then, else_))

    @check_pipeline
    def conditional(self, chained_condition: ChainedCondition):
        """
        Transforming stream elements on the basis of given condition.

        Example:
            conditions = (ChainedCondition().if_then(lambda x : x < 3, lambda x : 0)
                          .if_then(lambda x: x < 7,lambda x: 1)
                          .otherwise(lambda x : 2))

            Stream(range(10)).conditional(condition).collect(ToList())
            ->  [0, 0, 0, 1, 1, 1, 1, 2, 2, 2]

            conditions = (ChainedCondition().if_then(lambda x : x < 3, lambda x : 0)
              .if_then(lambda x: x < 7,lambda x: 1).done())

            Stream(range(10)).conditional(condition).collect(ToList())
            -> [0, 0, 0, 1, 1, 1, 1, 7, 8, 9]


        :param chained_condition:
        :return: Stream itself
        """

        return self.map(chained_condition)

    @check_pipeline
    def accumulate(self, bi_func: BiFunction[X, X, X]) -> 'Stream[X]':
        """
        accumulates stream elements using given "bi_func"

        Example:
            import operator as op

            Stream(range(10)).accumulate(op.add).collect(ToList())
            -> [0, 1, 3, 6, 10, 15, 21, 28, 36, 45]

            Stream(range(1,10)).accumulate(op.mul).collect(ToList())
            -> [1, 2, 6, 24, 120, 720, 5040, 40320, 362880]

        :param bi_func:
        :return: Stream itself
        """

        self._pointer = accumulate(self._pointer, bi_func)
        return self

    @check_pipeline
    def window_function(self, func, n: Union[int, None]) -> 'Stream[X]':
        """
        If "n" is not None (then it has to be an integer) then invoking function
        "func" on 'tuple' of "n" elements of stream. 'tuple' is made using past
        n-1 elements and 1 current element.

        If "n" is None, then all past values up to current value, held by a 'list',
        will be sent to function "func". Note that data type 'list', a mutable object,
        is chosen to make process more memory efficient. Any change made in the 'list'
        inside "func" will be visible to in future call to "func".(a 'list' is
        mutated via 'append','clean','extend','insert','pop','remove', 'reverse',
        'sort' methods.) Each call to "func" will be sent the same list appended
        with current element.

        Example1: Moving average for window size 3
            def mean(l): return sum(l)/len(l)

            Stream([1,6,2,7,3]).window_function(mean , 3).collect(ToList())
            -> [3.0, 5.0, 4.0]

        Example2: Averaging all past values:
            def mean(l): return sum(l)/len(l)

            Stream(range(1,5)).window_function(mean , None).collect(ToList())
            -> [1.0, 1.5, 2.0, 2.5]

        :param func: if "n" is not None then, takes input, at any instant,
                     as a 'tuple' having past "n-1" elements appended with
                     current element.

                     If "n" is None, the all past values in a list appended
                     with current element.
        :param n: natural number or None
        :return: Stream itself
        """

        if n is None:
            itr = Stream._all_past_values(self._pointer)
        else:
            itr = Stream._fetch_next(self._pointer, n)

        self._pointer = map(func, itr)
        return self

    @staticmethod
    def _all_past_values(itr: Iterable[X]) -> Iterable[list]:
        """
        Creates a generator.

        Generator always returns same list but each time an element from 'itr'
        is appended to the list.

        :param itr:
        :return:
        """

        data_holder = []

        for e in itr:
            data_holder.append(e)
            yield data_holder

    @staticmethod
    def _fetch_next(itr: Iterable[X], n: int) -> Iterable[Tuple[X, ...]]:
        """
        Creates generator. Each element is tuple of size "n".

        While iterating, first element will be first "n" elements of
        Stream, next element will be old "n-1" elements appended with
        next element of Stream and so on.

        Note that if in first call to this generator, stream has less
        than "n" element then ValueError will be thrown.

        :param itr:
        :param n: a natural number
        :return:
        """

        if n < 1:
            raise ValueError("'n' must be natural number")

        chunk = get_chunk(itr, n)

        if len(chunk) != n:
            # first chunk size must be "n".
            raise ValueError("Stream has less than '{}' elements ".format(n))

        yield chunk

        for e in itr:
            chunk = chunk[1:] + (e,)
            yield chunk

    def __next__(self) -> X:
        return next(self._pointer)

    # -------------------------- Terminal Operations ---------------------------

    @close_pipeline
    @check_pipeline
    def count(self) -> int:
        """
        This operation is one of the terminal operations.

        Finds number of elements in stream.

        Example:
            Stream(range(10)).count() -> 10

        :return: number of elements in Stream
        """

        return sum(1 for _ in self._pointer)

    @close_pipeline
    @check_pipeline
    def min(self, key=None) -> Optional[Any]:
        """
        This operation is one of the terminal operations
        finds minimum element in stream.

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

            Stream(students).min(key=Student.get_age) -> Optional[[name=B,age=1]]

        :param key:
        :return:
        """

        try:
            return Optional(min(self._pointer, key=key) if key else min(self._pointer))
        except ValueError:
            return EMPTY

    @close_pipeline
    @check_pipeline
    def max(self, key=None) -> Optional[Any]:
        """
        This operation is one of the terminal operations.
        finds maximum element in stream.

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

            Stream(students).max(key=Student.get_age) -> Optional[[name=D,age=6]]

        :param key:
        :return:
        """

        try:
            return Optional(max(self._pointer, key=key) if key else max(self._pointer))
        except ValueError:
            return EMPTY

    @close_pipeline
    @check_pipeline
    def all(self, predicate: Filter[X] = identity) -> bool:
        """
        This operation is one of the terminal operations.
        returns True if all elements returns True.

        Note that, if there is no element in stream then returns True.

        Example:
            class Student:
                def __init__(self, name, age):
                    self.name = name
                    self.age  = age

            Stream([Student('a',10), Student('b',12)]).all(predicate=lambda x:x.age < 15)
            -> True

            Stream([]).all() -> True # Empty Stream returns True
            Stream([0]).all() -> False
            Stream([1]).all() -> True

        :param predicate:
        :return:
        """

        return all(map(predicate, self._pointer))

    @close_pipeline
    @check_pipeline
    def any(self, predicate: Filter[X] = identity) -> bool:
        """
        This operation is one of the terminal operations
        Returns True if at-least one element are True according to given predicate.

        Note that if there is no element in the stream then returns False.

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

    @check_pipeline
    @close_pipeline
    def none_match(self, predicate: Filter[X] = identity) -> bool:
        """
        This operation is one of the terminal operations
        returns True if no element are true according to predicate.

        Note that if there is no element in the stream then returns True.

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

    @close_pipeline
    @check_pipeline
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

    @close_pipeline
    @check_pipeline
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

    @close_pipeline
    @check_pipeline
    def reduce(self, initial_point: X = NIL, *, bi_func: BiFunction[X, X, Y]) -> Optional[Y]:
        """
        This operation is one of the terminal operations
        reduces stream element to produce an element.

        Example:
            import operator as op

            Stream(range(1,6)).reduce(1,bi_func=op.mul).get() -> 120 (factorial 5)

        Case Without initial point(initial__pointer is NIL):
            Return value can only be EMPTY iff Stream does not having
            any element left in it.

            Stream([]).reduce(bi_func=op.add) -> EMPTY
            Stream([1]).reduce(bi_func=op.add) -> Optional[1]
            Stream([1, 2]).reduce(bi_func=op.add) -> Optional[3]

        Case With Initial Point (initial_point is not NIL):
            Return value will never be EMPTY.
            initial_point = 10

            Stream([]).reduce(initial_point,bi_func=op.add) -> Optional[10]
            Stream([1]).reduce(initial_point,bi_func=op.add) -> Optional[11]
            Stream([1, 2]).reduce(initial_point,bi_func=op.add) -> Optional[13]

        :param initial_point: defaults to NIL
        :param bi_func: reduction function
        :return:
        """

        if initial_point is not NIL:
            return Optional(reduce(bi_func, self._pointer, initial_point))
        else:
            try:
                return Optional(reduce(bi_func, self._pointer))
            except TypeError:
                return EMPTY

    @close_pipeline
    @check_pipeline
    def done(self):
        """
        This operation is one of the terminal operations.

        This can be used in case we are only interested in processing
        intermediate elements.

        """

        for _ in self._pointer:
            pass

    @close_pipeline
    @check_pipeline
    def collect(self, collector: Collector):
        """
        This operation is one of the terminal operations.
        For more detail see: streamAPI.stream.TO package.

        :param collector:
        :return:
        """

        for e in self._pointer:
            collector.consume(e)

        return collector.finish()

    @close_pipeline
    @check_pipeline
    def sum(self, start: 'X'):
        """
        Summing elements of stream.

        class Number:
            def __init__(self,num):
                self.num=num

            def __add__(self,other):
                return Number(self.num + other.num)

            def __str__(self):
                return str(self.num)

        data = Stream([Number(10),Number(20),Number(30)])
        start = Number(0)
        data.sum(start) -> 60

        :param start: starting point to start sum
        :return:
        """

        return sum(self._pointer, start)

    @close_pipeline
    @check_pipeline
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
