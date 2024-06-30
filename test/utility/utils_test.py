"""
author: Shiv
email: shivkj001@gmail.com
"""

import unittest
from datetime import date

from streamAPI import utility


class UtilityTest(unittest.TestCase):
    def test_divide_in_chunk(self):
        self.assertEqual(tuple(utility.divide_in_chunk(range(2, 12), 3)),
                         ((2, 3, 4), (5, 6, 7), (8, 9, 10), (11,)))

    def test_get_file_name(self):
        self.assertEqual(utility.get_file_name('a/b/c/d.text'), 'd')
        self.assertEqual(utility.get_file_name('a/b/c/d.text', -2), 'c')
        self.assertEqual(utility.get_file_name('a/b/c/d.text', 0), 'a')

    def test_filter_transform(self):
        self.assertTupleEqual(tuple(utility.filter_transform(range(10),
                                                             lambda x: x % 2,
                                                             lambda x: x + 1)),
                              (2, 4, 6, 8, 10))

    def test_as_date(self):
        self.assertEqual(utility.as_date('2017-1-1 00:12:31.912'), date(year=2017, month=1, day=1))
        self.assertEqual(utility.as_date('4-8-2017'), date(year=2017, month=4, day=8))

    def test_date_generator(self):
        sd = '2017-1-1'
        ed = '2017-1-10'
        self.assertTupleEqual(tuple(utility.date_generator(sd, ed, interval=2)),
                              (date(2017, 1, 1), date(2017, 1, 3), date(2017, 1, 5),
                               date(2017, 1, 7), date(2017, 1, 9)))


if __name__ == '__main__':
    unittest.main()
