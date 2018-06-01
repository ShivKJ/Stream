import unittest

from stream import Stream


class StreamTest(unittest.TestCase):
    def test_reduce(self):
        def _sum(a, b): return a + b

        print(Stream(range(3, 9)).reduce(_sum))
        self.assertEqual(Stream(range(3, 9)).reduce(_sum), sum(range(3, 9)))


if __name__ == '__main__':
    unittest.main()
