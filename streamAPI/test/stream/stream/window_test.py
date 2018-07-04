from unittest import TestCase, expectedFailure, main

from streamAPI.stream.stream import Stream


class WindowTest(TestCase):

    def test_1(self):
        out = Stream([1, 6, 2, 7, 3]).window_function(lambda l: sum(l) / 3, 3).as_seq()
        self.assertListEqual(out, [3, 5, 4])

    @expectedFailure
    def test_2(self):
        # Stream must be greater than 3
        Stream([1, 6]).window_function(lambda l: sum(l) / 3, 3).as_seq()


if __name__ == '__main__':
    main()
