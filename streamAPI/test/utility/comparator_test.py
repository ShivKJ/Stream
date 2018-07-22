"""
author: Shiv
email: shivkj001@gmail.com
"""

from collections import defaultdict
from operator import attrgetter
from unittest import TestCase, main

from streamAPI.stream import Optional, Stream
from streamAPI.stream.TO import GroupingBy, MaxBy, MinBy
from streamAPI.test.testHelper import random
from streamAPI.utility import comparing


class CompTest(TestCase):
    def test_1(self):
        rnd = random()
        data = rnd.int_range(1, 100, size=10)

        def mod_5(x: int): return x % 5

        bkt_max = Stream(data).collect(GroupingBy(mod_5, MaxBy()))

        temp = defaultdict(list)

        for e in data:
            temp[mod_5(e)].append(e)

        out_target = {k: Optional(max(v)) for k, v in temp.items()}

        self.assertDictEqual(bkt_max, out_target)

    def test_2(self):
        class Data:
            def __init__(self, num):
                self._num = num

            def __str__(self) -> str:
                return '[' + str(self._num) + ']'

            def __repr__(self):
                return str(self)

            def __eq__(self, other: 'Data'):
                return self._num == other._num

        rnd = random()

        data = [Data(x) for x in rnd.int_range(1, 100, size=10)]

        def mod_5(x: Data): return x._num % 5

        comp_key = attrgetter('_num')

        bkt_max = Stream(data).collect(GroupingBy(mod_5,
                                                  MaxBy(comparing(comp_key))))
        temp = defaultdict(list)

        for e in data:
            temp[mod_5(e)].append(e)

        out_target = {k: Optional(max(v, key=comp_key)) for k, v in temp.items()}

        self.assertDictEqual(bkt_max, out_target)

    def test_3(self):
        rnd = random()
        data = rnd.int_range(1, 100, size=10)

        def mod_5(x: int): return x % 5

        # We will be comparing result obtained by reversing comparator in MaxBy
        # so that we get "min" in each bucket created by "group by" operation; and
        # then comparing this result with the result obtained by MinBy.

        bkt_min = Stream(data).collect(GroupingBy(mod_5, MaxBy(comparing(lambda x: -x))))
        bkt_min_target = Stream(data).collect(GroupingBy(mod_5, MinBy()))

        self.assertDictEqual(bkt_min, bkt_min_target)

    def test_4(self):
        rnd = random()
        data = rnd.int_range(1, 100, size=10)

        def mod_5(x: int): return x % 5

        bkt_min = Stream(data).collect(GroupingBy(mod_5, MinBy()))

        temp = defaultdict(list)

        for e in data:
            temp[mod_5(e)].append(e)

        out_target = {k: Optional(min(v)) for k, v in temp.items()}

        self.assertDictEqual(bkt_min, out_target)


if __name__ == '__main__':
    main()
