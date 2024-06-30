"""
author: Shiv
email: shivkj001@gmail.com
"""

from unittest import TestCase, main

from streamAPI.stream import Stream
from streamAPI.stream.TO import ToList


class ExcludeTest(TestCase):
    def test_1(self):
        def is_even(x): return x % 2 == 0

        data = range(10)

        odd_numbers = Stream(data).exclude(is_even).collect(ToList())

        def is_odd(x): return x % 2 == 1

        out_target = [e for e in data if is_odd(e)]

        self.assertListEqual(odd_numbers, out_target)


if __name__ == '__main__':
    main()
