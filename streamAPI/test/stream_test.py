import unittest

from streamAPI.stream import EMPTY, ListType, SetType, Stream


class StreamTest(unittest.TestCase):
    def test_reduce(self):
        def _sum(a, b): return a + b

        self.assertEqual(Stream(range(3, 9)).reduce(_sum).get(), sum(range(3, 9)))
        self.assertTrue(Stream(range(0)).reduce(_sum) is EMPTY)

    def test_mapping(self):
        l = [5, 2, 5, 3, 4]
        out = Stream(l).mapping(lambda x: x, lambda x: x ** 2, resolve=lambda x, y: x + y)
        self.assertDictEqual(out, {5: 50, 2: 4, 3: 9, 4: 16})

        out = Stream([1, 2, 3, 4, 5, 6]).mapping(lambda x: x % 2, lambda x: x, lambda o, n: o + n)
        self.assertDictEqual(out, {1: 9, 0: 12})

    def test_batch(self):
        out = list(Stream(range(10)).batch(3))
        self.assertListEqual(out, [(0, 1, 2), (3, 4, 5), (6, 7, 8), (9,)])

    def test_enumerate(self):
        out = Stream(range(4, 10)).enumerate().as_seq()
        self.assertListEqual(out, [(0, 4), (1, 5), (2, 6), (3, 7), (4, 8), (5, 9)])

    def test_group_by(self):
        out = Stream(range(10)).group_by(key_hasher=lambda x: x % 3)
        self.assertDictEqual(out, {0: [0, 3, 6, 9], 1: [1, 4, 7], 2: [2, 5, 8]})

        out = Stream(range(10)).group_by(key_hasher=lambda x: x % 3, value_mapper=lambda x: x ** 2)
        self.assertDictEqual(out, {0: [0, 9, 36, 81], 1: [1, 16, 49], 2: [4, 25, 64]})

        out = Stream([1, 2, 3, 4, 2, 4]).group_by(lambda x: x % 2, value_container_clazz=ListType)
        self.assertDictEqual(out, {1: [1, 3], 0: [2, 4, 2, 4]})

        out = Stream([1, 2, 3, 4, 2, 4]).group_by(lambda x: x % 2, value_container_clazz=SetType)
        self.assertDictEqual(out, {1: {1, 3}, 0: {2, 4}})


if __name__ == '__main__':
    unittest.main()
