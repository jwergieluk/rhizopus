import unittest

from rhizopus.price_graph import find_path, calc_path_price


class TestUtils(unittest.TestCase):
    def setUp(self):
        self.spread = 0.95
        self.prices = {
            ('EUR', 'USD'): 1.2 * self.spread,
            ('USD', 'EUR'): self.spread * 1.0 / 1.2,
            ('XAU', 'USD'): self.spread * 1000.0 * 1.2,
            ('USD', 'XAU'): self.spread * 1.0 / (1000.0 * 1.2),
        }

    def test_find_path1(self):
        edges = [('EUR', 'USD'), ('USD', 'JPY'), ('JPY', 'CHF')]
        self.assertEqual(find_path(edges, [], 'EUR', 'CHF'), edges)

    def test_find_path2(self):
        edges = [('EUR', 'USD'), ('USD', 'JPY'), ('USD', 'EUR'), ('JPY', 'USD')]
        path = find_path(edges, [], 'JPY', 'EUR')
        self.assertTrue(len(path) > 1)
        self.assertEqual(path[0][0], 'JPY')
        self.assertEqual(path[-1][1], 'EUR')
        for p1, p2 in zip(path[:-1], path[1:]):
            self.assertEqual(p1[1], p2[0])

    def test_path_price(self):
        for num0, num1 in self.prices.keys():
            self.assertEqual(calc_path_price(self.prices, num0, num1), self.prices[(num0, num1)])

        self.assertAlmostEqual(
            calc_path_price(self.prices, 'XAU', 'EUR'),
            self.prices[('XAU', 'USD')] * self.prices[('USD', 'EUR')],
        )

        self.assertAlmostEqual(
            calc_path_price(self.prices, 'EUR', 'XAU'),
            self.prices[('EUR', 'USD')] * self.prices[('USD', 'XAU')],
        )

        self.assertIsNone(calc_path_price(self.prices, 'XAU', 'ETH'))
