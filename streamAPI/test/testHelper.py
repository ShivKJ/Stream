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


# -----------------------------------------------------------------
def x_eq_y(x, y) -> bool: return x == y


def eq(y): return partial(x_eq_y, y=y)


def x_lt_y(x, y) -> bool: return x < y


def lt(y): return partial(x_lt_y, y=y)


def x_lte_y(x, y) -> bool: return x <= y


def lte(y): return partial(x_lte_y, y=y)


def x_gt_y(x, y) -> bool: return x > y


def gt(y): return partial(x_gt_y, y=y)


def x_gte_y(x, y) -> bool: return x >= y


def gte(y): return partial(x_gte_y, y=y)


def x_ne_y(x, y) -> bool: return x != y


def ne(y): return partial(x_ne_y, y=y)


def x_gte_y_lte_z(x, y, z): return y <= x <= z


def gte_y_lte_z(y, z): return partial(x_gte_y_lte_z, y=y, z=z)


def x_gt_y_lte_z(x, y, z): return y < x <= z


def gt_y_lte_z(y, z): return partial(x_gt_y_lte_z, y=y, z=z)


def x_gte_y_lt_z(x, y, z): return y <= x < z


def gte_y_lt_z(y, z): return partial(x_gte_y_lt_z, y=y, z=z)


def x_gt_y_lt_z(x, y, z): return y < x < z


def gt_y_lt_z(y, z): return partial(x_gt_y_lt_z, y=y, z=z)


# -----------------------------------
def equal(self, target, x) -> bool: return self(x) == target


def not_equal(self, target, x) -> bool: return self(x) != target


def less_than(self, target, x) -> bool: return self(x) < target


def greater_than(self, target, x) -> bool: return self(x) > target


def and_(self, other, x) -> bool: return self(x) and other(x)


def or_(self, other, x) -> bool: return self(x) or other(x)


def not_(self, x) -> bool: return not self(x)


def sum_xy(x, y):
    return x + y


def multiply_xy(x, y):
    return x * y


def multiply(*args):
    assert len(args) > 0, 'must have at least one element'

    out = 1

    for e in args: out *= e

    return out
