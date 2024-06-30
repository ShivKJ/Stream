"""
author: Shiv
email: shivkj001@gmail.com
"""

# This module implements some of the useful implementations of "Collector"
# abstract class.

# Currently there are following implementations:
# 1)  ToList: Collects stream elements into a "list"
# 2)  ToLinkedList: Collects stream elements into a "deque"
# 3)  ToSet: Collects stream elements into "set"
# 4)  CollectAndThen: firstly holds elements into a container using a downstream operation,
#                     and then applies a function on it.
# 5)  ToMap: Creates a dictionary from stream elements.
# 6)  Mapping: transforms stream elements and then collect then using downstream operation.
# 7)  MaxBy: Finds max element from stream (output will be of type "Optional")
# 8)  MinBy: Finds min element from stream (output will be of type "Optional")
# 9)  Joining: Joins string stream using separator "sep", "prefix" and "suffix".
# 10) Counting: finds number of element in stream.
# 11) Summing: sums elements of stream.
# 12) Averaging: finds average of elements of stream.
# 13) Reduce: Reduces stream elements using Binary function "bi_func". (output will be of type "Optional")
# 14) GroupingBy: groups stream elements into bucket (keys in dictionary are referred as buckets.).

from abc import ABC, abstractmethod
from collections import defaultdict, deque
from typing import Any, DefaultDict, Union

from streamAPI.stream.optional import Optional, create_optional
from streamAPI.utility.Types import BiFunction, Function, X
from streamAPI.utility.utils import NIL, default_comp, get_functions_clazz, identity


class Collector(ABC):
    """
    Object of this class will be used in Stream.collect method.
    """

    @abstractmethod
    def supply(self) -> 'Collector':
        """
        supplies a new Collector.
        :return:
        """

    @abstractmethod
    def consume(self, e):
        """
        Defines how to process a data "e".
        :param e:
        """

    @abstractmethod
    def finish(self):
        """
        defines what to return with "consumed" elements.
        :return:
        """


# ------------------------------------------------------------------
class DataHolder(Collector):
    """
    This class will be used to hold stream data into
    container such as 'list','set','deque' etc.
    """

    def __init__(self, container_class):
        super().__init__()

        self._data_holder = container_class()
        self._cls = container_class

    def supply(self) -> Collector:
        return self.__class__()

    @abstractmethod
    def consume(self, e): pass

    def finish(self):
        return self._data_holder


class ToList(DataHolder):
    """
    Puts elements into a 'list'.

    Stream(range(5)).collect(ToList()) -> [0, 1, 2, 3, 4]
    """

    def __init__(self):
        super().__init__(container_class=list)

    def consume(self, e):
        self._data_holder.append(e)


class ToLinkedList(DataHolder):
    """
    Puts elements into a 'deque'
    Stream(range(5)).collect(ToLinkedList()) -> deque([0, 1, 2, 3, 4])
    """

    def __init__(self):
        super().__init__(container_class=deque)

    def consume(self, e):
        self._data_holder.append(e)


class ToSet(DataHolder):
    """
    Puts elements into a 'set'.
    Stream([1,4,2,6,1,5,6]).collect(ToSet()) -> {1, 2, 4, 5, 6}
    """

    def __init__(self):
        super().__init__(container_class=set)

    def consume(self, e):
        self._data_holder.add(e)


# ------------------------------------------------------------------

class CollectAndThen(Collector):
    """
    Collects elements using "downstream" and then applies the "then" function.

    Stream([1,4,2,6,1,5,6]).collect(CollectAndThen(ToSet(),sum)) -> 18
    Stream([1,4,2,6,1,5,6]).collect(CollectAndThen(ToList(),sum)) -> 25

    """

    def __init__(self, downstream: Collector, then):
        self._downstream = downstream
        self._then = then

    def supply(self) -> Collector:
        return CollectAndThen(self._downstream.supply(), self._then)

    def consume(self, e):
        self._downstream.consume(e)

    def finish(self):
        return self._then(self._downstream.finish())


def on_conflict_do_nothing(o, n):
    """
    :param o: old value
    :param n: new value
    :return: old value
    """
    return o


class ToMap(Collector):
    """
    Used to create map from data points.

    Stream(range(5)).collect(ToMap(lambda x:x,lambda x:x**2))
    -> {0: 0, 1: 1, 2: 4, 3: 9, 4: 16}

    Stream([1,4,2,6,1,5,6]).collect(ToMap(lambda x:x,lambda x:x**2,
                                          merger_on_conflict=lambda o,n:o))
    -> {1: 1, 4: 16, 2: 4, 6: 36, 5: 25}

    """

    def __init__(self, key_mapper: Function, value_mapper: Function = identity,
                 merger_on_conflict: BiFunction = None):
        super().__init__()

        self._key_mapper = key_mapper
        self._value_mapper = value_mapper
        self._merger_on_conflict = merger_on_conflict

        self._data_holder = {}

    def supply(self) -> Collector:
        return ToMap(self._key_mapper, self._value_mapper, self._merger_on_conflict)

    def consume(self, e):
        """
        creates an Entry in with key using function '_key_mapper' and value
        using function '_value_mapper'. Note that these key-value pairs are stored
        in a dictionary so key made must to hashable.

        In case, key has already been created with earlier element, a ValueError
        will be thrown if "_merge_on_conflict" is not set, else this BiFunction
        will be used to resolve the conflict.

        Example:
            from streamAPI.stream import *
            from streamAPI.stream.TO import *

            Stream([1,2,3,4]).collect(ToMap(lambda x:x,lambda x:x**2))
            -> {1: 1, 2: 4, 3: 9, 4: 16}

            Stream([1,2,1,4]).collect(ToMap(lambda x:x,lambda x:x**2))
            will throw ValueError as element '1' is present multiple times.


            Stream([1,2,1,4]).collect(ToMap(lambda x:x,lambda x:x**2,lambda o,n:o))
            -> {1: 1, 2: 4, 4: 16} # in case of conflict, we are using old value.


        :param e:
        :return:
        """

        bkt = self._key_mapper(e)

        if bkt in self._data_holder:
            if self._merger_on_conflict is None:
                raise ValueError(f'k : {bkt} is already present.')

            old_v = self._data_holder[bkt]

            self._data_holder[bkt] = self._merger_on_conflict(old_v, self._value_mapper(e))
        else:
            self._data_holder[bkt] = self._value_mapper(e)

    def finish(self) -> dict:
        return self._data_holder


class Mapping(Collector):
    """
    Maps elements using "func" function first, then collects transformed
    elements according to "downstream".

    Stream(range(5)).collect(Mapping(lambda x:x**2,Summing()))
    -> 30 # sum of squares
    """

    def __init__(self, func: Function, downstream: Collector = None):
        super().__init__()

        self._func = func
        self._downstream = downstream or ToList()

    def supply(self) -> Collector:
        return Mapping(self._func, self._downstream.supply())

    def consume(self, e):
        self._downstream.consume(self._func(e))

    def finish(self):
        return self._downstream.finish()


class MaxBy(Collector):
    """
    Finds max element using comparator "comp"
    Stream([1,4,2,6,1,5,6]).collect(MaxBy()) -> Optional[6]
    """

    def __init__(self, comp: BiFunction[X, X, int] = default_comp):
        super().__init__()

        self._max = NIL
        self._comp = comp

    def supply(self) -> Collector:
        return MaxBy(self._comp)

    def consume(self, e):
        if self._max is NIL:
            self._max = e
        elif self._comp(e, self._max) > 0:
            self._max = e

    def finish(self) -> Optional:
        return create_optional(self._max)


class MinBy(Collector):
    """
    Finds min element using comparator "comp"
    Stream([1,4,2,6,1,5,6]).collect(MinBy()) -> Optional[1]
    """

    def __init__(self, comp: BiFunction[X, X, int] = default_comp):
        super().__init__()

        self._comp = comp
        self._min = NIL

    def supply(self) -> Collector:
        return MinBy(self._comp)

    def consume(self, e):
        if self._min is NIL:
            self._min = e
        elif self._comp(e, self._min) < 0:
            self._min = e

    def finish(self) -> Optional:
        return create_optional(self._min)


class Joining(ToLinkedList):
    """
    Joins string elements.

    Stream(['A','B','C']).collect(Joining(',','<','>')) -> '<A,B,C>'
    """

    def __init__(self, sep: str = '', prefix: str = '', suffix: str = ''):
        super().__init__()

        self._sep = sep
        self._prefix = prefix
        self._suffix = suffix

    def supply(self) -> Collector:
        return Joining(self._sep, self._prefix, self._suffix)

    def finish(self) -> str:
        return '{}{}{}'.format(self._prefix,
                               self._sep.join(self._data_holder),
                               self._suffix)


class Counting(Collector):
    """
    Counts number of elements in Stream.

    Stream(range(5)).collect(Counting()) -> 5
    """

    def __init__(self):
        super().__init__()

        self._count = 0

    def supply(self) -> Collector:
        return Counting()

    def consume(self, e):
        self._count += 1

    def finish(self) -> int:
        return self._count


class Summing(Collector):
    """
    Sums element of Stream.
    Stream(range(5)).collect(Summing()) -> 10

    """

    def __init__(self):
        super().__init__()

        self._sum = 0

    def supply(self) -> Collector:
        return Summing()

    def consume(self, e):
        self._sum += e

    def finish(self) -> Union[int, float]:
        return self._sum


class Averaging(Summing, Counting):
    """
    Finds average of elements.
    Stream(range(5)).collect(Averaging()) -> 2.0
    """

    def __init__(self):
        super().__init__()

    def supply(self) -> Collector:
        return Averaging()

    def consume(self, e):
        Summing.consume(self, e)
        Counting.consume(self, e)

    def finish(self) -> float:
        return self._sum / self._count


class Reduce(Collector):
    """
    Reduces stream data.

    import operator as op
    Stream(range(1,5)).collect(Reduce(bi_func=op.mul)) -> Optional[24]
    """

    def __init__(self, o=NIL, *, bi_func: BiFunction):
        """
        Reduces Stream data.

        :param o: initial point, defaults to NIL
        :param bi_func: reducing function

        """

        super().__init__()

        self._o = o
        self._bi_func = bi_func

        self._data_holder = o

    def supply(self) -> Collector:
        return Reduce(self._o, bi_func=self._bi_func)

    def consume(self, e):
        if self._data_holder is NIL:
            self._data_holder = e
        else:
            self._data_holder = self._bi_func(self._data_holder, e)

    def finish(self) -> Optional:
        return create_optional(self._data_holder)


class GroupingBy(Collector):
    """
    Groups stream elements.

    Stream(['AB','BD','AC','DE','BD','BW','AB']).collect(GroupingBy(lambda x:x[0],
                                                                    GroupingBy(lambda x:x[1],
                                                                                Counting())))
    -> {'A': {'B': 2, 'C': 1}, 'B': {'D': 2, 'W': 1}, 'D': {'E': 1}}
    """

    def __init__(self, group_by: Function, downstream: Collector = None):
        super().__init__()

        self._group_by = group_by
        self._downstream = downstream or ToList()
        self._bucket: DefaultDict[Any, Collector] = defaultdict(self._downstream.supply)

    def supply(self) -> Collector:
        return GroupingBy(self._group_by, self._downstream.supply())

    def consume(self, e):
        bkt = self._group_by(e)
        self._bucket[bkt].consume(e)

    def finish(self) -> dict:
        return {k: v.finish() for k, v in self._bucket.items()}


if __name__ == 'streamAPI.stream.TO.TerminalOperations':
    __all__ = get_functions_clazz(__name__, __file__)
