"""
author: Shiv
email: shivkj001@gmail.com
"""

import operator as op
from collections import defaultdict
from unittest import TestCase, main

from streamAPI.stream import Stream
from streamAPI.stream.TO import Counting, GroupingBy, Mapping, ToList, ToMap, ToSet
from streamAPI.stream.optional import EMPTY, Optional
from streamAPI.stream.streamHelper import ChainedCondition
from streamAPI.test.testHelper import random
from streamAPI.utility.utils import identity


class StreamTest(TestCase):
    def test_reduce(self):
        def _sum(a, b): return a + b

        self.assertEqual(Stream(range(3, 9)).reduce(bi_func=op.add).get(), sum(range(3, 9)))
        self.assertTrue(Stream(range(0)).reduce(bi_func=op.add) is EMPTY)

        self.assertEqual(Stream([]).reduce(bi_func=op.add), EMPTY)
        self.assertEqual(Stream([1]).reduce(bi_func=op.add), Optional(1))
        self.assertEqual(Stream([1, 2]).reduce(bi_func=op.add), Optional(3))

        initial_point = 10

        self.assertEqual(Stream([]).reduce(bi_func=_sum, initial_point=initial_point), Optional(10))
        self.assertEqual(Stream([1]).reduce(bi_func=_sum, initial_point=initial_point), Optional(11))
        self.assertEqual(Stream([1, 2]).reduce(bi_func=_sum, initial_point=initial_point), Optional(13))

    def test_mapping(self):
        l = [5, 2, 5, 3, 4]

        out = Stream(l).collect(ToMap(identity, lambda x: x ** 2, merger_on_conflict=op.add))
        self.assertDictEqual(out, {5: 50, 2: 4, 3: 9, 4: 16})

        out = Stream([1, 2, 3, 4, 5, 6]).collect(ToMap(lambda x: x % 2, merger_on_conflict=op.add))
        self.assertDictEqual(out, {1: 9, 0: 12})

    def test_batch(self):
        out = list(Stream(range(10)).batch(3))
        self.assertListEqual(out, [(0, 1, 2), (3, 4, 5), (6, 7, 8), (9,)])

    def test_enumerate(self):
        out = Stream(range(4, 10)).enumerate().collect(ToList())
        self.assertListEqual(out, [(0, 4), (1, 5), (2, 6), (3, 7), (4, 8), (5, 9)])

    def test_group_by(self):

        out = Stream(range(10)).collect(GroupingBy(lambda x: x % 3))
        self.assertDictEqual(out, {0: [0, 3, 6, 9], 1: [1, 4, 7], 2: [2, 5, 8]})

        out = Stream(range(10)).collect(GroupingBy(lambda x: x % 3, Mapping(lambda x: x ** 2)))
        self.assertDictEqual(out, {0: [0, 9, 36, 81], 1: [1, 16, 49], 2: [4, 25, 64]})

        out = Stream([1, 2, 3, 4, 2, 4]).collect(GroupingBy(lambda x: x % 2))
        self.assertDictEqual(out, {1: [1, 3], 0: [2, 4, 2, 4]})

        out = Stream([1, 2, 3, 4, 2, 4]).collect(GroupingBy(lambda x: x % 2, ToSet()))
        self.assertDictEqual(out, {1: {1, 3}, 0: {2, 4}})

    def test_zip(self):
        out = Stream([4, 1, 2, 3]).zip(range(9), range(2, 9)).collect(ToList())
        self.assertListEqual(out, [(4, 0, 2), (1, 1, 3), (2, 2, 4), (3, 3, 5)])

        out = Stream([4, 1, 2, 3]).zip(range(9), range(2, 9), after=False).collect(ToList())
        self.assertListEqual(out, [(0, 2, 4), (1, 3, 1), (2, 4, 2), (3, 5, 3)])

    def test_zip_longest(self):
        out = Stream([4, 1, 2, 3]).zip_longest(range(9), range(2, 9), fillvalue=-1).collect(ToList())
        self.assertListEqual(out, [(4, 0, 2),
                                   (1, 1, 3),
                                   (2, 2, 4),
                                   (3, 3, 5),
                                   (-1, 4, 6),
                                   (-1, 5, 7),
                                   (-1, 6, 8),
                                   (-1, 7, -1),
                                   (-1, 8, -1)])

        out = Stream([4, 1, 2, 3]).zip_longest(range(9), range(2, 9), fillvalue=-1).collect(ToList())

        self.assertListEqual(out, [(4, 0, 2),
                                   (1, 1, 3),
                                   (2, 2, 4),
                                   (3, 3, 5),
                                   (-1, 4, 6),
                                   (-1, 5, 7),
                                   (-1, 6, 8),
                                   (-1, 7, -1),
                                   (-1, 8, -1)])

        out = Stream([4, 1, 2, 3]).zip_longest(range(9), range(2, 9), after=False, fillvalue=None).collect(ToList())
        self.assertListEqual(out, [(0, 2, 4),
                                   (1, 3, 1),
                                   (2, 4, 2),
                                   (3, 5, 3),
                                   (4, 6, None),
                                   (5, 7, None),
                                   (6, 8, None),
                                   (7, None, None),
                                   (8, None, None)]
                             )

    def test_cycle(self):
        out = Stream([4, 1, 2, 3, 9, 0, 5]).cycle(range(3)).collect(ToList())
        self.assertListEqual(out, [(4, 0), (1, 1), (2, 2), (3, 0), (9, 1), (0, 2), (5, 0)])

        out = Stream([4, 1, 2, 3, 9, 0, 5]).cycle(range(3), after=False).collect(ToList())
        self.assertListEqual(out, [(0, 4), (1, 1), (2, 2), (0, 3), (1, 9), (2, 0), (0, 5)])

    def test_if_else(self):
        rnd = random()

        def get(): return rnd.randint(1, 100)

        size = 1000

        out = (Stream.from_supplier(get)
               .limit(size)
               .if_else(lambda x: x < 50, lambda x: 0, lambda x: 1)
               .collect(GroupingBy(identity, Counting())))

        out_target = defaultdict(int)

        rnd.reset()

        for _ in range(size):
            out_target[0 if get() < 50 else 1] += 1

        self.assertDictEqual(out, out_target)

    def test_conditional1(self):
        rnd = random()

        def get():
            return rnd.randint(1, 100)

        size = 1000

        conditions = (ChainedCondition()
                      .if_then(lambda x: x <= 10, lambda x: 10)
                      .if_then(lambda x: x <= 20, lambda x: 20)
                      .if_then(lambda x: x <= 30, lambda x: 30)
                      .done())

        out = (Stream.from_supplier(get)
               .limit(size)
               .conditional(conditions)
               .collect(GroupingBy(identity, Counting())))

        out_target = defaultdict(int)
        rnd.reset()

        for _ in range(size):
            e = get()
            k = None

            if e <= 10:
                k = 10
            elif e <= 20:
                k = 20
            elif e <= 30:
                k = 30
            else:
                k = e

            out_target[k] += 1

        self.assertDictEqual(out, out_target)

    def test_conditional2(self):
        rnd = random()

        def get():
            return rnd.randint(1, 100)

        size = 1000

        conditions = (ChainedCondition()
                      .if_then(lambda x: x <= 10, lambda x: 10)
                      .if_then(lambda x: x <= 20, lambda x: 20)
                      .if_then(lambda x: x <= 30, lambda x: 30)
                      .otherwise(lambda x: -1))

        out = (Stream.from_supplier(get)
               .limit(size)
               .conditional(conditions)
               .collect(GroupingBy(identity, Counting())))

        out_target = defaultdict(int)
        rnd.reset()

        for _ in range(size):
            e = get()

            if e <= 10:
                k = 10
            elif e <= 20:
                k = 20
            elif e <= 30:
                k = 30
            else:
                k = -1

            out_target[k] += 1

        self.assertDictEqual(out, out_target)


if __name__ == '__main__':
    main()
