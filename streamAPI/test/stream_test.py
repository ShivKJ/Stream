import unittest

from streamAPI.stream import EMPTY, Stream


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


if __name__ == '__main__':
    unittest.main()
