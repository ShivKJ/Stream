"""
author: Shiv
email: shivkj001@gmail.com
"""

from unittest import TestCase, expectedFailure, main

from streamAPI.stream.optional import EMPTY, Optional


class OptionalClassTest(TestCase):
    def test_EMPTY(self):
        self.assertNotEqual(EMPTY, Optional(None))
        self.assertNotEqual(Optional(None), EMPTY)
        self.assertNotEqual(EMPTY, Optional(1))
        self.assertNotEqual(Optional(1), EMPTY)

    def test_present(self):
        self.assertFalse(EMPTY.present())
        self.assertTrue(Optional(None).present())
        self.assertTrue(Optional(1).present())

    def test_get1(self):
        self.assertIsNone(Optional(None).get())
        self.assertEqual(Optional(1).get(), 1)

    @expectedFailure
    def test_get2(self):
        self.assertIsNone(EMPTY.get())

    def test_or_else(self):
        self.assertEqual(EMPTY.or_else(1), 1)
        self.assertIsNone(Optional(None).or_else(10))
        self.assertEqual(Optional(10).or_else(1), 10)

    @expectedFailure
    def test_or_raise1(self):
        self.assertIsNone(EMPTY.or_raise(ValueError()))

    def test_or_raise2(self):
        self.assertIsNone(Optional(None).or_raise(ValueError()))
        self.assertEqual(Optional(1).or_raise(ValueError()), 1)


if __name__ == '__main__':
    main()
