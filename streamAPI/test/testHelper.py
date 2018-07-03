from abc import ABC, abstractmethod
from functools import partial
from random import Random
from sys import version as VERSION

from streamAPI.stream.streamHelper import Supplier

SEED = 10


class RND(Random):
    def __init__(self, x=None, seed=SEED, version=VERSION):
        super().__init__(x=x)

        self._seed = seed
        self._version = version

    def reset(self):
        self.seed(self._seed, self._seed)

    def int_range(self, a, b, step=1, *, size) -> list:
        return [self.randrange(a, b, step) for _ in range(size)]

    def int_range_supplier(self, a, b, step=1) -> Supplier:
        return Supplier(lambda: self.randrange(a, b, step))


def random(seed=SEED, version=VERSION) -> RND:
    rnd = RND()
    rnd.seed(seed, version)

    return rnd


def equal(self, target, x) -> bool: return self(x) == target


def not_equal(self, target, x) -> bool: return self(x) != target


def less_than(self, target, x) -> bool: return self(x) < target


def greater_than(self, target, x) -> bool: return self(x) > target


def and_(self, other, x) -> bool: return self(x) and other(x)


def or_(self, other, x) -> bool: return self(x) or other(x)


def not_(self, x) -> bool: return not self(x)


class Logic(ABC):
    def __init__(self, logic=None):
        self._logic = logic

    @abstractmethod
    def __call__(self, y) -> bool: return self._logic(y)

    def equal_to(self, y): return Logic(partial(equal, self, y))

    def not_equal_to(self, y): return Logic(partial(not_equal, self, y))

    def less_than(self, y): return Logic(partial(less_than, self, y))

    def greater_than(self, y): return Logic(partial(greater_than, self, y))

    def and_(self, other): return Logic(partial(and_, self, other))

    def or_(self, other): return Logic(partial(or_, self, other))

    def not_(self): return Logic(partial(not_, self))


class Adder(Logic):
    def __init__(self, a):
        super().__init__(None)

        self._a = a

    def __call__(self, x):
        return self._a + x


class Pow(Logic):
    def __init__(self, a):
        super().__init__(None)

        self._a = a

    def __call__(self, x): return x ** self._a


class Modulus(Logic):
    def __init__(self, a):
        super().__init__(None)

        self._a = a

    def __call__(self, x): return x % self._a
