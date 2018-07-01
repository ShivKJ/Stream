from random import Random
from sys import version as VERSION

SEED = 10


def random(seed=SEED, version=VERSION):
    rnd = Random()
    rnd.seed(seed, version)

    return rnd
