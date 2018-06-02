import unittest

from stream import Stream, EMPTY


class StreamTest(unittest.TestCase):
    def test_reduce(self):
        def _sum(a, b): return a + b

        self.assertEqual(Stream(range(3, 9)).reduce(_sum).get(), sum(range(3, 9)))
        self.assertTrue(Stream(range(0)).reduce(_sum) is EMPTY)


if __name__ == '__main__':
    unittest.main()
