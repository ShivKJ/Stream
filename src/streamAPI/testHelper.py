"""
author: Shiv
email: shivkj001@gmail.com
"""

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

    def float_range(self, a, b, size) -> list:
        return [self.uniform(a, b) for _ in range(size)]

    def int_range_supplier(self, a, b, step=1) -> Supplier:
        return Supplier(lambda: self.randrange(a, b, step))


def random(seed=SEED, version=VERSION) -> RND:
    rnd = RND()
    rnd.seed(seed, version)

    return rnd
