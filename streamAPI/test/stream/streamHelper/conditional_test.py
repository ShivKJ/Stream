"""
author: Shiv
email: shivkj001@gmail.com
"""

from unittest import TestCase, expectedFailure, main

from streamAPI.stream.streamHelper import ChainedCondition
from streamAPI.utility.utils import identity


class TestingConditional(TestCase):
    def __init__(self, methodName='runTest'):
        super().__init__(methodName=methodName)
        self.chained_condition = ChainedCondition()

    def tearDown(self):
        self.chained_condition = None

    @expectedFailure
    def test_done1(self):
        self.chained_condition.done().if_then(lambda x: x < 5, identity)

    @expectedFailure
    def test_done2(self):
        self.chained_condition.done().otherwise(identity)

    def test_done3(self):
        self.chained_condition \
            .if_then(if_=lambda e: e < 5,
                     then=lambda e: e ** 2) \
            .done()

        out = [self.chained_condition.apply(e) for e in range(10)]
        out_target = [e ** 2 if e < 5 else e for e in range(10)]

        self.assertListEqual(out, out_target)

    def test_done4(self):
        self.chained_condition \
            .if_then(if_=lambda e: e < 5,
                     then=lambda e: e ** 2) \
            .if_then(if_=lambda e: e < 8,
                     then=lambda e: e + 2) \
            .done()

        out = (self.chained_condition.apply(e) for e in range(10))

        for o, e in zip(out, range(10)):
            if e < 5:
                e **= 2
            elif e < 8:
                e += 2

            with self.subTest(e=e):
                self.assertEqual(o, e)

    @expectedFailure
    def test_otherwise1(self):
        self.chained_condition.otherwise(identity).done()

    @expectedFailure
    def test_otherwise2(self):
        self.chained_condition.otherwise(identity).if_then(lambda x: x < 10, identity)

    def test_otherwise3(self):
        self.chained_condition \
            .if_then(if_=lambda e: e < 5,
                     then=lambda e: e ** 2) \
            .otherwise(lambda x: x + 1)

        out = [self.chained_condition.apply(e) for e in range(10)]
        out_target = [e ** 2 if e < 5 else e + 1 for e in range(10)]

        self.assertListEqual(out, out_target)

    def test_otherwise4(self):
        self.chained_condition \
            .if_then(if_=lambda e: e < 5,
                     then=lambda e: e ** 2) \
            .if_then(if_=lambda e: e < 8,
                     then=lambda e: e + 2) \
            .otherwise(lambda x: x % 3)

        out = (self.chained_condition.apply(e) for e in range(10))

        for o, e in zip(out, range(10)):
            if e < 5:
                e **= 2
            elif e < 8:
                e += 2
            else:
                e %= 3

            with self.subTest(e=e):
                self.assertEqual(o, e)


if __name__ == '__main__':
    main()
