import unittest

from streamAPI.stream import EMPTY, ListType, Optional, SetType, Stream


class StreamTest(unittest.TestCase):
    def test_reduce(self):
        def _sum(a, b): return a + b

        self.assertEqual(Stream(range(3, 9)).reduce(_sum).get(), sum(range(3, 9)))
        self.assertTrue(Stream(range(0)).reduce(_sum) is EMPTY)

        self.assertEqual(Stream([]).reduce(_sum), EMPTY)
        self.assertEqual(Stream([1]).reduce(_sum), Optional(1))
        self.assertEqual(Stream([1, 2]).reduce(_sum), Optional(3))

        initial_point = 10

        self.assertEqual(Stream([]).reduce(_sum, initial_point=initial_point), Optional(10))
        self.assertEqual(Stream([1]).reduce(_sum, initial_point=initial_point), Optional(11))
        self.assertEqual(Stream([1, 2]).reduce(_sum, initial_point=initial_point), Optional(13))

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

    def test_zip(self):
        out = Stream([4, 1, 2, 3]).zip(range(9), range(2, 9)).as_seq()
        self.assertListEqual(out, [(4, 0, 2), (1, 1, 3), (2, 2, 4), (3, 3, 5)])

        out = Stream([4, 1, 2, 3]).zip(range(9), range(2, 9), after=False).as_seq()
        self.assertListEqual(out, [(0, 2, 4), (1, 3, 1), (2, 4, 2), (3, 5, 3)])

    def test_zip_longest(self):
        out = Stream([4, 1, 2, 3]).zip_longest(range(9), range(2, 9), fillvalue=-1).as_seq()
        self.assertListEqual(out, [(4, 0, 2),
                                   (1, 1, 3),
                                   (2, 2, 4),
                                   (3, 3, 5),
                                   (-1, 4, 6),
                                   (-1, 5, 7),
                                   (-1, 6, 8),
                                   (-1, 7, -1),
                                   (-1, 8, -1)])

        out = Stream([4, 1, 2, 3]).zip_longest(range(9), range(2, 9), fillvalue=-1).as_seq()

        self.assertListEqual(out, [(4, 0, 2),
                                   (1, 1, 3),
                                   (2, 2, 4),
                                   (3, 3, 5),
                                   (-1, 4, 6),
                                   (-1, 5, 7),
                                   (-1, 6, 8),
                                   (-1, 7, -1),
                                   (-1, 8, -1)])

        out = Stream([4, 1, 2, 3]).zip_longest(range(9), range(2, 9), after=False, fillvalue=None).as_seq()
        self.assertListEqual(out, [(0, 2, 4),
                                   (1, 3, 1),
                                   (2, 4, 2),
                                   (3, 5, 3),
                                   (4, 6, None),
                                   (5, 7, None),
                                   (6, 8, None),
                                   (7, None, None),
                                   (8, None, None)]
                             )

    def test_cycle(self):
        out = Stream([4, 1, 2, 3, 9, 0, 5]).cycle(range(3)).as_seq()
        self.assertListEqual(out, [(4, 0), (1, 1), (2, 2), (3, 0), (9, 1), (0, 2), (5, 0)])

        out = Stream([4, 1, 2, 3, 9, 0, 5]).cycle(range(3), after=False).as_seq()
        self.assertListEqual(out, [(0, 4), (1, 1), (2, 2), (0, 3), (1, 9), (2, 0), (0, 5)])


if __name__ == '__main__':
    unittest.main()
