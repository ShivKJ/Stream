"""
author: Shiv
email: shivkj001@gmail.com
"""

from itertools import islice
from unittest import TestCase, main

from streamAPI.stream.stream import Stream


class Sum(TestCase):

    def test_1(self):
        class Number:
            def __init__(self, num):
                self._num = num

            @property
            def num(self):
                return self._num

            def __add__(self, other: 'Number') -> 'Number':
                self._num += other._num  # mutating first class object.

                return self

            def __str__(self):
                return str(self._num)

            def __eq__(self, other: 'Number') -> bool:
                return self._num == other._num

        data = tuple(Number(x) for x in range(10))

        out = Stream(data).filter(lambda x: x.num % 2 == 0).limit(3).sum(Number(0))

        out_target = sum(islice((e for e in data if e.num % 2 == 0), 3),
                         Number(0))

        self.assertEqual(out, out_target)


if __name__ == '__main__':
    main()
