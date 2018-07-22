"""
author: Shiv
email: shivkj001@gmail.com
"""

import operator as op
from itertools import accumulate
from unittest import TestCase, main

from streamAPI.stream import Stream
from streamAPI.stream.TO import ToList


class AccumulateTest(TestCase):
    def test_accumulate1(self):
        data = range(10)
        out = Stream(data).accumulate(op.add).collect(ToList())

        out_target = list(accumulate(data, op.add))
        self.assertListEqual(out, out_target)

    def test_accumulate2(self):
        data = [True, 1, 5, 0.1, 'a', ['b'], {'c'}, {'de': 'f'}]

        out = Stream(data).accumulate(lambda x, y: x and y).all()

        self.assertIs(out, all(data))

    def test_accumulate3(self):
        data = [False, frozenset(), 0, 1, 5, 0.1, '',
                ['a'], {}, {'bc': 'd'}, (), 0.0]

        out = Stream(data).accumulate(lambda a, b: a or b).any()

        self.assertIs(out, any(data))

    def test_accumulate4(self):
        size = 10

        data = range(size)

        out = Stream(data).accumulate(op.add).reduce(bi_func=op.add).get()

        self.assertEqual(out, sum(sum(range(i)) for i in range(1, size + 1)))

    def test_accumulate5(self):
        size = 10

        data = range(size)

        out = Stream(data).map(lambda x: x ** 2).accumulate(op.add).reduce(bi_func=op.add).get()

        self.assertEqual(out, sum(sum(j ** 2 for j in range(i)) for i in range(1, size + 1)))


if __name__ == '__main__':
    main()
