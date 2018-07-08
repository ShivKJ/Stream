from collections import Counter, defaultdict
from operator import attrgetter
from unittest import TestCase, main

from streamAPI.stream import Stream
from streamAPI.stream.TO import (CollectAndThen, Counting, GroupingBy, Mapping, Summing,
                                 ToList, ToSet)
from streamAPI.test.testHelper import random
from streamAPI.utility import identity


class GroupByTest(TestCase):
    def test_1(self):
        rnd = random()

        start, end = 1, 100
        size = 1000

        data = rnd.int_range(start, end, size=size)
        element_count = Stream(data).collect(GroupingBy(identity,
                                                        CollectAndThen(ToList(), len)))

        out_target = Counter(data)
        self.assertDictEqual(element_count, out_target)

    def test_1a(self):
        rnd = random()

        start, end = 1, 100
        size = 1000

        data = rnd.int_range(start, end, size=size)
        element_count = Stream(data).collect(GroupingBy(identity, Counting()))

        out_target = Counter(data)
        self.assertDictEqual(element_count, out_target)

    def test_2(self):
        rnd = random()

        start, end = 1, 100
        size = 1000

        def mod_10(x): return x % 10

        data = rnd.int_range(start, end, size=size)

        distinct_elements = Stream(data).collect(GroupingBy(mod_10, ToSet()))

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

        out = Stream(data).collect(GroupingBy(mod_10, ToList()))

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

        out = Stream(data).collect(GroupingBy(mod_10, Summing()))

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

        out = Stream(data).collect(GroupingBy(mod_10, Mapping(square, Summing())))

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

        element_distinct_count = Stream(data).collect(GroupingBy(mod_100,
                                                                 CollectAndThen(ToSet(), len)))

        temp = defaultdict(set)

        for e in data:
            temp[mod_100(e)].add(e)

        out_target = {k: len(es) for k, es in temp.items()}

        self.assertDictEqual(element_distinct_count, out_target)

    def test_7(self):
        def mod_10(e): return e % 10

        self.assertDictEqual(Stream(()).collect(GroupingBy(mod_10)), dict())

    def test_8(self):
        rnd = random()
        data_size = 1000
        countries = tuple('ABC')
        sex = ('M', 'F')

        class Person:
            def __init__(self, country, state, age, sex):
                self.country = country
                self.state = state
                self.age = age
                self.sex = sex  # male or female

        ps = [Person(rnd.choice(countries), rnd.randrange(1, 4), rnd.randrange(0, 60), rnd.choice(sex))
              for _ in range(data_size)]

        collector = GroupingBy(attrgetter('country'),
                               GroupingBy(attrgetter('state'),
                                          GroupingBy(attrgetter('sex'),
                                                     Counting())))

        flt = lambda x: 20 <= x.age <= 50

        out = Stream(ps).filter(flt).collect(collector)

        out_target = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

        for p in ps:
            if flt(p):
                out_target[p.country][p.state][p.sex] += 1

        self.assertDictEqual(out, out_target)


if __name__ == '__main__':
    main()
