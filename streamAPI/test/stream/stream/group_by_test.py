from collections import Counter, defaultdict
from unittest import TestCase, main

from streamAPI.stream import ListType, SetType, Stream
from streamAPI.test.testHelper import random
from streamAPI.utility import identity


class GroupByTest(TestCase):
    def test_1(self):
        rnd = random()

        start, end = 1, 100
        size = 1000

        data = rnd.int_range(start, end, size=size)
        element_count = Stream(data).group_by(identity, collect_and_then=len)

        out_target = Counter(data)
        self.assertDictEqual(element_count, out_target)

    def test_2(self):
        rnd = random()

        start, end = 1, 100
        size = 1000

        def mod_10(x): return x % 10

        data = rnd.int_range(start, end, size=size)

        distinct_elements = Stream(data).group_by(mod_10, bucket_type=SetType)

        out_target = defaultdict(set)

        for e in data:
            out_target[mod_10(e)].add(e)

        self.assertDictEqual(distinct_elements, out_target)

    def test_3(self):
        rnd = random()

        start, end = 1, 100
        size = 1000

        def mod_10(e): return e % 10

        data = rnd.int_range(start, end, size=size)

        out = Stream(data).group_by(mod_10, bucket_type=ListType)

        out_target = defaultdict(list)

        for e in data:
            out_target[mod_10(e)].append(e)

        self.assertDictEqual(out, out_target)

    def test_4(self):
        rnd = random()

        start, end = 1, 100
        size = 1000

        def mod_10(e): return e % 10

        data = rnd.int_range(start, end, size=size)

        out = Stream(data).group_by(mod_10, bucket_type=ListType, collect_and_then=sum)

        out_target = defaultdict(int)

        for e in data:
            out_target[mod_10(e)] += e

        self.assertDictEqual(out, out_target)

    def test_5(self):
        rnd = random()

        start, end = 1, 100
        size = 1000

        def mod_10(e): return e % 10

        def square(e): return e ** 2

        data = rnd.int_range(start, end, size=size)

        out = Stream(data).group_by(mod_10, square,
                                    bucket_type=ListType, collect_and_then=sum)

        out_target = defaultdict(int)

        for e in data:
            out_target[mod_10(e)] += square(e)

        self.assertDictEqual(out, out_target)

    def test_6(self):
        rnd = random()

        start, end = 1, 10000
        size = 1000

        def mod_100(x): return x % 100

        data = rnd.int_range(start, end, size=size)

        element_distinct_count = Stream(data).group_by(mod_100,
                                                       bucket_type=SetType,
                                                       collect_and_then=len)

        temp = defaultdict(set)

        for e in data:
            temp[mod_100(e)].add(e)

        out_target = {k: len(es) for k, es in temp.items()}

        self.assertDictEqual(element_distinct_count, out_target)

    def test_7(self):
        def mod_10(e): return e % 10

        self.assertDictEqual(Stream(()).group_by(mod_10), dict())


if __name__ == '__main__':
    main()
