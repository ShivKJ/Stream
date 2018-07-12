from collections import defaultdict
from operator import attrgetter
from unittest import TestCase, main

from streamAPI.stream import Optional, Stream
from streamAPI.stream.TO import GroupingBy, MaxBy
from streamAPI.test.testHelper import random
from streamAPI.utility import comparing


class CompTest(TestCase):
    def test_1(self):
        rnd = random()
        data = rnd.int_range(1, 100, size=10)

        mod_5 = lambda x: x % 5

        bkt_max = Stream(data).collect(GroupingBy(mod_5, MaxBy()))
        temp = defaultdict(list)

        for e in data:
            temp[mod_5(e)].append(e)

        out_target = {k: Optional(max(v)) for k, v in temp.items()}

        self.assertDictEqual(bkt_max, out_target)

    def test_2(self):
        class Number:
            def __init__(self, num):
                self.num = num

            def __str__(self) -> str:
                return '[' + str(self.num) + ']'

            def __repr__(self):
                return str(self)

            def __eq__(self, other: 'Number'):
                return self.num == other.num

        rnd = random()

        data = [Number(x) for x in rnd.int_range(1, 100, size=10)]

        mod_5 = lambda x: x.num % 5

        comp_key = attrgetter('num')

        bkt_max = Stream(data).collect(GroupingBy(mod_5,
                                                  MaxBy(comparing(comp_key))))
        temp = defaultdict(list)

        for e in data:
            temp[mod_5(e)].append(e)

        out_target = {k: Optional(max(v, key=comp_key)) for k, v in temp.items()}

        self.assertDictEqual(bkt_max, out_target)


if __name__ == '__main__':
    main()
