"""
author: Shiv
email: shivkj001@gmail.com
"""

from functools import total_ordering
from unittest import TestCase, main

from streamAPI.stream import Stream
from streamAPI.stream.TO import ToList
from streamAPI.test.testHelper import random


class SortTest(TestCase):
    def test_1(self):
        size = 1000

        rnd = random()

        square = lambda x: x ** 2

        out = (Stream.from_supplier(rnd.random)
               .limit(size)
               .map(square)
               .sort()
               .collect(ToList()))

        rnd.reset()

        out_target = sorted(square(rnd.random()) for _ in range(size))

        for o, t in zip(out, out_target):
            with self.subTest(o=o):
                self.assertAlmostEqual(o, t, delta=1e-9)

    def test_2(self):
        size = 1000

        rnd = random()

        square = lambda x: x ** 2

        out = (Stream.from_supplier(rnd.random)
               .limit(size)
               .map(square)
               .sort(reverse=True)
               .collect(ToList()))

        rnd.reset()

        out_target = sorted((square(rnd.random()) for _ in range(size)), reverse=True)
        for o, t in zip(out, out_target):
            with self.subTest(o=o):
                self.assertAlmostEqual(o, t, delta=1e-9)

    def test_3(self):
        size = 10
        eps = 1e-9

        @total_ordering
        class Data:
            def __init__(self, e):
                self._e = e

            def __eq__(self, other) -> bool:
                return abs(self._e - other._e) <= eps

            def __lt__(self, other) -> bool:
                return self._e < other._e

        rnd = random()

        out = (Stream.from_supplier(rnd.random)
               .limit(size)
               .map(Data)
               .sort(reverse=True)
               .collect(ToList()))

        rnd.reset()

        out_target = sorted((Data(rnd.random()) for _ in range(size)), reverse=True)

        for o, t in zip(out, out_target):
            with self.subTest(o=o):
                self.assertAlmostEqual(o, t)

    def test_4(self):
        size = 10
        eps = 1e-9

        class Data:
            def __init__(self, e):
                self._e = e

            def e(self):
                return self._e

            def __eq__(self, other) -> bool:
                return abs(self._e - other._e) <= eps

        rnd = random()

        out = (Stream.from_supplier(rnd.random)
               .limit(size)
               .map(Data)
               .sort(key=Data.e, reverse=True)
               .collect(ToList()))

        rnd.reset()

        out_target = sorted((Data(rnd.random()) for _ in range(size)),
                            key=Data.e,
                            reverse=True)

        for o, t in zip(out, out_target):
            with self.subTest(o=o):
                self.assertAlmostEqual(o, t)


if __name__ == '__main__':
    main()
