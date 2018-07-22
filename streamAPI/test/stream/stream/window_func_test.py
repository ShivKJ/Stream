"""
author: Shiv
email: shivkj001@gmail.com
"""

from decimal import Context, Decimal as D
from statistics import mean, pstdev
from typing import List
from unittest import TestCase, expectedFailure, main

from streamAPI.stream import Stream
from streamAPI.stream.TO import ToList
from streamAPI.test.testHelper import random


class MyTestCase(TestCase):
    def test_1(self):
        rnd = random()

        data = rnd.int_range(1, 100, size=1000)

        def mean(es):
            return sum(es) / len(es)

        window_size = 4

        out = Stream(data).window_function(mean, window_size).collect(ToList())

        out_target = []

        chunk = data[:window_size]

        out_target.append(mean(chunk))

        for e in data[window_size:]:
            chunk = chunk[1:] + [e]
            out_target.append(mean(chunk))

        for o, t in zip(out, out_target):
            with self.subTest():
                self.assertAlmostEqual(o, t, delta=1e-8)

    @expectedFailure
    def test_2(self):
        def mean(es):
            return sum(es) / len(es)

        Stream(range(3)).window_function(mean, 5).done()
        # throws Value error as total number of elements in stream is less than window size.

    def test_3(self):
        data = range(100)

        def mean(l):
            return sum(l) / len(l)

        out = Stream(data).window_function(mean, None).collect(ToList())

        val = data[0]
        eps = 1e-8

        self.assertAlmostEqual(out[0], val, delta=eps)

        for n, e in enumerate(data[1:], start=1):
            val = (n * val + e) / (n + 1)

            with self.subTest():
                self.assertAlmostEqual(out[n], val, delta=eps)

    def test_4(self):
        data = random().int_range(1, 100, size=200)

        class Statistic:
            def __init__(self):
                self._mean = 0
                self._mean_sqr = 0  # mean of squares
                self._n = 0

            @property
            def mean(self):
                return self._mean

            @property
            def std(self):
                return (self._mean_sqr - self.mean ** 2) ** 0.5

            def __call__(self, es: list):
                n = self._n
                e = es[-1]

                self._mean = (self._mean * n + e) / (n + 1)
                self._mean_sqr = (self._mean_sqr * n + e ** 2) / (n + 1)

                self._n = n + 1
                return dict(mean=self.mean, std=self.std)

        stat = Statistic()

        out: List[dict] = Stream(data).window_function(stat, None).collect(ToList())

        eps = 1e-9

        data_holder = []

        for o, d in zip(out, data):
            data_holder.append(d)

            with self.subTest():
                mu = mean(data_holder)
                self.assertAlmostEqual(o['mean'], mu, delta=eps)
                self.assertAlmostEqual(o['std'], pstdev(data_holder, mu=mu), delta=eps)

    def test_5(self):

        ctx = Context(prec=40)

        def dot_product(v1, v2):
            l1, l2 = len(v1), len(v2)

            assert l1 == l2, "dimension mismatch; v1's dim: {} and v2's dim: {}".format(l1, l2)
            assert v1 and v2, 'each vector must have at least one element.'

            out = 0

            for x, y in zip(v1, v2):
                out += x * y

            return out

        class WeightedAverage:
            def __init__(self, weight: tuple):
                assert sum(weight) == 1, 'weight array is not normalised'

                self._weight = weight

            def __call__(self, es: tuple):
                return dot_product(self._weight, es)

        # -------------------------------------------------------------------
        data = tuple(D(e, ctx) for e in random().float_range(1, 100, size=1000))
        # -------------------------------------------------------------------

        alpha = D(0.1, ctx)
        window_size = 10

        _sum = alpha * (1 - alpha ** window_size) / (1 - alpha)
        weight = tuple(alpha ** n / _sum for n in range(1, window_size + 1))  # making sum(weight) == 1

        wa = WeightedAverage(weight)

        # -------------------------------------------------------------------
        out = Stream(data).window_function(wa, window_size).collect(ToList())
        # -------------------------------------------------------------------

        chunk = data[:window_size]

        self.assertEqual(out[0], dot_product(chunk, weight))

        for n, e in enumerate(data[window_size:], start=1):
            chunk = chunk[1:] + (e,)
            self.assertEqual(out[n], dot_product(chunk, weight))


if __name__ == '__main__':
    main()
