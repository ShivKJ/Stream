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


if __name__ == '__main__':
    unittest.main()
