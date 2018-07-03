from unittest import TestCase, main

from streamAPI.stream.stream import Stream
from streamAPI.test.testHelper import Adder, Modulus, Pow, random


class Map(TestCase):
    def setUp(self):
        self.rnd = random()

    def tearDown(self):
        self.rnd = None

    def test_map1(self):
        rnd = self.rnd
        a, b, size = 1, 100, 1000

        add_5 = Adder(5)
        pow_3 = Pow(3)

        out = (Stream(rnd.int_range_supplier(a, b))
               .limit(size)
               .map(add_5)
               .map(pow_3)
               .as_seq())

        rnd.reset()

        out_target = [pow_3(add_5(e)) for e in rnd.int_range(a, b, size=size)]

        self.assertListEqual(out, out_target)

    def test_map2(self):
        rnd = self.rnd
        a, b, size = 1, 100, 1000

        add_5 = Adder(5)
        pow_3 = Pow(3)
        mod_2 = Modulus(2)
        mod_3 = Modulus(3)
        mod_5 = Modulus(5)

        out = (Stream(rnd.int_range_supplier(a, b))
               .limit(size)
               .filter(lambda e: mod_3(e) != 0)
               .map(add_5)
               .filter(mod_2)
               .map(pow_3)
               .filter(lambda e: mod_5(e) != 0)
               .as_seq())

        rnd.reset()

        out_target = []

        for e in rnd.int_range(a, b, size=size):
            if mod_3(e) != 0:
                e = add_5(e)

                if mod_2(e):
                    e = pow_3(e)

                    if mod_5(e) != 0:
                        out_target.append(e)

        self.assertListEqual(out, out_target)

    def test_map3(self):
        rnd = self.rnd
        a, b, size = 1, 100, 1000

        pow_2 = Pow(2)

        out = sum(Stream(rnd.int_range_supplier(a, b))
                  .limit(size)
                  .map(pow_2))

        rnd.reset()

        out_target = sum(pow_2(e) for e in rnd.int_range(a, b, size=size))

        self.assertEqual(out, out_target)


if __name__ == '__main__':
    main()
