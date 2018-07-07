from abc import ABC, abstractmethod
from collections import deque
from typing import Any, Dict

from streamAPI.stream.optional import EMPTY, Optional
from streamAPI.utility.Types import BiFunction, Function, X
from streamAPI.utility.utils import NIL, default_comp


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


class DataHolder(Collector):
    def __init__(self, cls):
        super().__init__()

        self._data_holder = cls()
        self._cls = cls

    def supply(self):
        return DataHolder._supply(self._data_holder)

    @classmethod
    def _supply(cls, _holder):
        return cls(_holder)

    @abstractmethod
    def consume(self, e): pass

    def finish(self):
        return self._data_holder


class ListType(DataHolder):
    def __init__(self, cls=list):
        super().__init__(cls=cls)

    def consume(self, e):
        self._data_holder.append(e)


class ToSet(DataHolder):
    def __init__(self):
        super().__init__(cls=set)

    def consume(self, e):
        self._data_holder.add(e)


class CollectAndThen(Collector):
    def __init__(self, collector: Collector, func):
        self._collector = collector
        self._func = func

    def supply(self):
        return CollectAndThen(self._collector.supply(), self._func)

    def consume(self, e):
        self._collector.consume(e)

    def finish(self):
        return self._func(self._collector.finish())


class ToMap(Collector):
    def __init__(self, km: Function, vm: Function, merger: BiFunction = None):
        self._key_mapper = km
        self._value_mapper = vm
        self._merger_on_conflict = merger

        self._data_holder = {}

    def supply(self):
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

    def finish(self):
        return self._data_holder


class Mapping(Collector):
    def __init__(self, func: Function, downstream: Collector):
        self._func = func
        self._downstream = downstream

    def supply(self):
        return Mapping(self._func, self._downstream.supply())

    def consume(self, e):
        self._downstream.consume(self._func(e))

    def finish(self):
        return self._downstream.finish()


class MaxBy(Collector):
    def __init__(self, comp: BiFunction[X, X, int] = default_comp):
        self._max = NIL
        self._comp = comp

    def supply(self):
        return MaxBy(self._comp)

    def consume(self, e):
        if self._max is NIL:
            self._max = e
        elif self._comp(e, self._max) > 0:
            self._max = e

    def finish(self):
        return self._max


class MinBy(Collector):
    def __init__(self, comp: BiFunction[X, X, int] = default_comp):
        self._comp = comp
        self._min = NIL

    def supply(self):
        return MinBy(self._comp)

    def consume(self, e):
        if self._min is NIL:
            self._min = e
        elif self._comp(e, self._min) < 0:
            self._min = e

    def finish(self):
        return self._min


class Joining(DataHolder):
    def __init__(self, sep='', prefix='', suffix=''):
        super().__init__(deque)
        self.sep = sep
        self.prefix = prefix
        self.suffix = suffix

    def consume(self, e):
        self._data_holder.append(e)

    def finish(self):
        return '{}{}{}'.format(self.prefix, self.sep.join(self._data_holder), self.suffix)


class Counting(Collector):
    def __init__(self):
        self._count = 0

    def supply(self):
        return Counting()

    def consume(self, e):
        self._count += 1

    def finish(self):
        return self._count


class Reduce(Collector):
    def __init__(self, o=NIL, *, bi_func: BiFunction):
        self._o = o
        self._bi_func = bi_func

        self._data_holder = o

    def supply(self):
        return Reduce(self._o, bi_func=self._bi_func)

    def consume(self, e):
        if self._data_holder is NIL:
            self._data_holder = e
        else:
            self._data_holder = self._bi_func(self._data_holder, e)

    def finish(self):
        e = self._data_holder

        return Optional(e) if e is not NIL else EMPTY


class GroupingBy(Collector):
    def __init__(self, grp: Function, downstream: Collector = None):
        self._group_by = grp
        self._bucket: Dict[Any, Collector] = {}

        if downstream is None:
            self._downstream = ListType()
        else:
            self._downstream = downstream

    def supply(self):
        return GroupingBy(self._group_by, self._downstream.supply())

    def _get(self, bkt):
        if bkt not in self._bucket:
            ptr = self._downstream.supply()
            self._bucket[bkt] = ptr
        else:
            ptr = self._bucket[bkt]

        return ptr

    def consume(self, e):
        self._get(self._group_by(e)).consume(e)

    def finish(self):
        return {k: v.finish() for k, v in self._bucket.items()}
