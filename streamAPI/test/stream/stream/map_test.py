"""
author: Shiv
email: shivkj001@gmail.com
"""

import operator as op
from functools import reduce
from unittest import TestCase, main

from streamAPI.stream import Stream
from streamAPI.stream.TO import ToList
from streamAPI.stream.streamHelper import ChainedCondition
from streamAPI.test.testHelper import random


class Map(TestCase):
    def setUp(self):
        self.rnd = random()

    def tearDown(self):
        self.rnd = None

    def test_map1(self):
        rnd = self.rnd
        a, b, size = 1, 100, 1000

        add_5 = lambda x: x + 5
        pow_3 = lambda x: x ** 3

        out = (Stream(rnd.int_range_supplier(a, b))
               .limit(size)
               .map(add_5)
               .map(pow_3)
               .collect(ToList()))

        rnd.reset()

        out_target = [pow_3(add_5(e)) for e in rnd.int_range(a, b, size=size)]

        self.assertListEqual(out, out_target)

    def test_map2(self):
        rnd = self.rnd
        a, b, size = 1, 100, 1000

        add_5 = lambda x: x + 5
        pow_3 = lambda x: x ** 3

        mod_2_not_zero = lambda x: x % 2 != 0
        mod_3_not_2 = lambda x: x % 3 != 2
        mod_5_not_4 = lambda x: x % 5 != 4

        out = (Stream(rnd.int_range_supplier(a, b))
               .limit(size)
               .filter(mod_3_not_2)
               .map(add_5)
               .filter(mod_2_not_zero)
               .map(pow_3)
               .filter(mod_5_not_4)
               .collect(ToList()))

        rnd.reset()

        out_target = []

        for e in rnd.int_range(a, b, size=size):
            if mod_3_not_2(e):
                e = add_5(e)

                if mod_2_not_zero(e):
                    e = pow_3(e)

                    if mod_5_not_4(e):
                        out_target.append(e)

        self.assertListEqual(out, out_target)

    def test_map3(self):
        rnd = self.rnd
        a, b, size = 1, 100, 1000

        pow_2 = lambda x: x ** 2

        out = sum(Stream(rnd.int_range_supplier(a, b))
                  .limit(size)
                  .map(pow_2))

        rnd.reset()

        out_target = sum(pow_2(e) for e in rnd.int_range(a, b, size=size))

        self.assertEqual(out, out_target)

    def test_map4(self):
        rnd = self.rnd
        a, b, size = 1, 100, 1000

        pow_2 = lambda x: x ** 2

        out = Stream(rnd.int_range_supplier(a, b)) \
            .limit(size) \
            .map(pow_2) \
            .reduce(0, bi_func=op.add) \
            .get()

        rnd.reset()

        out_target = sum(pow_2(e) for e in rnd.int_range(a, b, size=size))

        self.assertEqual(out, out_target)

    def test_map5(self):
        rnd = self.rnd
        a, b, size = 1, 100, 50

        out = Stream(rnd.int_range_supplier(a, b)) \
            .limit(size) \
            .reduce(1, bi_func=op.mul) \
            .get()

        rnd.reset()

        out_target = reduce(op.mul, rnd.int_range(a, b, size=size))

        self.assertEqual(out, out_target)

    def test_map6(self):
        rnd = self.rnd
        a, b, size = 1, 100, 50

        cc = (ChainedCondition()
              .if_then(lambda x: x < 50, lambda x: x ** 2)
              .done())

        out = Stream(rnd.int_range_supplier(a, b)) \
            .limit(size) \
            .conditional(cc) \
            .reduce(1, bi_func=op.mul) \
            .get()

        rnd.reset()

        data = (cc(e) for e in rnd.int_range(a, b, size=size))

        out_target = reduce(op.mul, data)

        self.assertEqual(out, out_target)

    def test_map7(self):
        rnd = self.rnd
        a, b, size = 1, 100, 50

        cc = (ChainedCondition.if_else(lambda x: x < 50, lambda x: x ** 2, lambda x: x % 50))

        out = Stream(rnd.int_range_supplier(a, b)) \
            .limit(size) \
            .conditional(cc) \
            .reduce(1, bi_func=op.mul) \
            .get()

        rnd.reset()

        data = (cc(e) for e in rnd.int_range(a, b, size=size))

        out_target = reduce(op.mul, data)

        self.assertEqual(out, out_target)

    def test_map8(self):
        rnd = self.rnd
        a, b, size = 1, 100, 50

        rng = range(3, 70)

        out = (Stream(rnd.int_range_supplier(a, b))
               .limit(size)
               .zip(rng)
               .map(sum)
               .reduce(1, bi_func=op.mul)
               .get())

        rnd.reset()

        data = (e + r for e, r in zip(rnd.int_range(a, b, size=size), rng))

        out_target = reduce(op.mul, data)

        self.assertEqual(out, out_target)


if __name__ == '__main__':
    main()
