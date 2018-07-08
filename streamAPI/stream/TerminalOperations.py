from abc import ABC, abstractmethod
from collections import deque
from typing import Any, Dict, Union

from streamAPI.stream.optional import EMPTY, Optional
from streamAPI.utility.Types import BiFunction, Function, X
from streamAPI.utility.utils import NIL, default_comp, get_functions_clazz, identity


class Collector(ABC):
    @abstractmethod
    def supply(self) -> 'Collector':
        pass

    @abstractmethod
    def consume(self, e):
        pass

    @abstractmethod
    def finish(self):
        pass


# ------------------------------------------------------------------
class DataHolder(Collector):
    """
    This class will be used to hold stream data into
    container such as 'list','set','deque' etc.
    """

    def __init__(self, cls):
        super().__init__()

        self._data_holder = cls()
        self._cls = cls

    @abstractmethod
    def supply(self) -> Collector: pass

    @abstractmethod
    def consume(self, e): pass

    def finish(self):
        return self._data_holder


class ToList(DataHolder):
    """
    Puts elements into a 'list'.

    Stream(range(5)).collect(ToList()) -> [0, 1, 2, 3, 4]
    """

    def __init__(self, cls=list):
        super().__init__(cls)

    def supply(self):
        return ToList(self._cls)

    def consume(self, e):
        self._data_holder.append(e)


class ToLinkedList(ToList):
    """
    Puts elements into a 'deque'
    Stream(range(5)).collect(ToLinkedList()) -> deque([0, 1, 2, 3, 4])
    """

    def __init__(self):
        super().__init__(deque)


class ToSet(DataHolder):
    """
    Puts elements into a 'set'.
    Stream([1,4,2,6,1,5,6]).collect(ToSet()) -> {1, 2, 4, 5, 6}
    """

    def __init__(self):
        super().__init__(cls=set)

    def supply(self):
        return ToSet()

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
        bkt = self._key_mapper(e)

        if bkt in self._data_holder:
            if self._merger_on_conflict is None:
                raise ValueError('k : {} is already present.'.format(bkt))

            old_v = self._data_holder[bkt]

            self._data_holder[bkt] = self._merger_on_conflict(old_v, self._value_mapper(e))
        else:
            self._data_holder[bkt] = self._value_mapper(e)

    def finish(self) -> dict:
        return self._data_holder


class Mapping(Collector):
    """
    Maps using "func" function first, then collects elements
    according to "downstream".

    Stream(range(5)).collect(Mapping(lambda x:x**2,Summing())) -> 30

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
        e = self._max

        return Optional(e) if e is not NIL else EMPTY


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
        e = self._min

        return Optional(e) if e is not NIL else EMPTY


class Joining(ToLinkedList):
    """
    Joins string elements.

    Stream(['A','B','C']).collect(Joining(',','<','>')) -> '<A,B,C>'
    """

    def __init__(self, sep='', prefix='', suffix=''):
        super().__init__()

        self._sep = sep
        self._prefix = prefix
        self._suffix = suffix

    def supply(self) -> Collector:
        return Joining(self._sep, self._prefix, self._suffix)

    def finish(self) -> str:
        return '{}{}{}'.format(self._prefix, self._sep.join(self._data_holder), self._suffix)


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
        e = self._data_holder

        return Optional(e) if e is not NIL else EMPTY


class GroupingBy(Collector):
    """
    Groups stream elements.

    Stream(['AB','BD','AC','DE','BD','BW','AB']).collect(GroupingBy(lambda x:x[0],
                                                                    GroupingBy(lambda x:x[1],
                                                                                Counting())))
    -> {'A': {'B': 2, 'C': 1}, 'B': {'D': 2, 'W': 1}, 'D': {'E': 1}}
    """

    def __init__(self, grp: Function, downstream: Collector = None):
        self._group_by = grp
        self._bucket: Dict[Any, Collector] = {}

        if downstream is None:
            self._downstream = ToList()
        else:
            self._downstream = downstream

    def supply(self) -> Collector:
        return GroupingBy(self._group_by, self._downstream.supply())

    def _get(self, bkt) -> Collector:
        if bkt not in self._bucket:
            ptr = self._downstream.supply()
            self._bucket[bkt] = ptr
        else:
            ptr = self._bucket[bkt]

        return ptr

    def consume(self, e):
        self._get(self._group_by(e)).consume(e)

    def finish(self) -> dict:
        return {k: v.finish() for k, v in self._bucket.items()}


if __name__ == 'streamAPI.stream.TerminalOperations':
    __all__ = get_functions_clazz(__name__, __file__)
