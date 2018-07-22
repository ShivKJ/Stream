"""
author: Shiv
email: shivkj001@gmail.com
"""

from unittest import TestCase, expectedFailure, main

from streamAPI.test.testHelper import random
from streamAPI.utility.utils import get_chunk


class ChunkTest(TestCase):
    def test_1(self):
        rnd = random()
        a, b, size = 0, 10, 10
        chunk_size = 3

        data = rnd.int_range(a, b, size=size)
        itr = iter(data.copy())

        for i in range(a, b, chunk_size):
            chunk = get_chunk(itr, chunk_size, list)
            self.assertListEqual(chunk, data[i:i + chunk_size])

    def test_2(self):
        data = ()
        self.assertEqual(get_chunk(data, 3), ())

    @expectedFailure
    def test_3(self):
        data = range(10)
        get_chunk(data, 3.5)  # should throw exception

    @expectedFailure
    def test_4(self):
        data = range(10)
        get_chunk(data, None)  # should throw exception

    @expectedFailure
    def test_5(self):
        data = range(10)
        get_chunk(data, 0)  # should throw exception

    @expectedFailure
    def test_6(self):
        data = range(10)
        get_chunk(data, range(0))  # should throw exception

    @expectedFailure
    def test_7(self):
        data = range(10)
        get_chunk(data, range(3, 3))  # should throw exception

    @expectedFailure
    def test_8(self):
        data = range(10)
        get_chunk(data, range(5, 3))  # should throw exception


if __name__ == '__main__':
    main()
