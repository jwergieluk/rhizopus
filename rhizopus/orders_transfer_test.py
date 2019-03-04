import unittest
from . import BrokerState, ForwardTransferOrder, BackwardTransferOrder


class TransferOrderTest0(unittest.TestCase):
    def test_one_num_transfer(self):
        accounts = {'CASH1': (10.0, 'EUR'), 'CASH2': (-20.0, 'EUR')}
        broker_state1 = BrokerState('XAU', accounts)
        # broker_state2 = BrokerState('XAU', accounts)

        order1 = BackwardTransferOrder('CASH1', 'CASH2', (1.0, 'EUR'))
        # order2 = order1.inversed()

        order1.execute(broker_state1)
        # order2.execute(broker_state2)

        # self.assertEqual(vars(broker_state1), vars(broker_state2))
        self.assertEqual(broker_state1.accounts['CASH1'], (9.0, 'EUR'))
        self.assertEqual(broker_state1.accounts['CASH2'], (-19.0, 'EUR'))


class TransferOrderTest1(unittest.TestCase):
    def setUp(self):
        self.accounts = {'EUR_CASH': (10000.0, 'EUR'), 'USD_CASH': (10000.0, 'USD'), 'XAU_BARS': (1.0, 'XAU')}
        self.spread = 0.95
        self.prices = {('EUR', 'USD'): 1.2 * self.spread,
                       ('USD', 'EUR'): self.spread * 1.0 / 1.2,
                       ('XAU', 'EUR'): 1000.0 * self.spread,
                       ('EUR', 'XAU'): self.spread * 1.0 / 1000.0,
                       ('XAU', 'USD'): self.spread * 1000.0 * 1.2,
                       ('USD', 'XAU'): self.spread * 1.0 / (1000.0 * 1.2)}
        self.broker_state1 = BrokerState('EUR', self.accounts)
        self.broker_state1.current_prices.update(self.prices)

        self.broker_state2 = BrokerState('EUR', self.accounts)
        self.broker_state2.current_prices.update(self.prices)

    def test_price_noarb(self):
        values = []
        capital = 100.0
        for i in range(10):
            capital = capital*self.prices[('EUR', 'USD')]*self.prices[('USD', 'EUR')]
            values.append(capital)

        for i in range(1, len(values)):
            self.assertLess(values[i], values[i-1])

    def test_basic1(self):
        order1 = BackwardTransferOrder('EUR_CASH', 'USD_CASH', (1.0, 'USD'))
        order1.execute(self.broker_state1)
        self.assertEqual(self.broker_state1.accounts['USD_CASH'], (10001.0, 'USD'))
        self.assertEqual(self.broker_state1.accounts['EUR_CASH'],
                         (10000.0 - 1.0/self.prices[('EUR', 'USD')], 'EUR'))

    def test_basic2(self):
        order1 = ForwardTransferOrder('EUR_CASH', 'USD_CASH', (1.0, 'EUR'))
        order1.execute(self.broker_state1)
        self.assertEqual(self.broker_state1.accounts['EUR_CASH'], (9999.0, 'EUR'))
        self.assertEqual(self.broker_state1.accounts['USD_CASH'],
                         (10000.0 + self.prices[('EUR', 'USD')], 'USD'))

    def test_basic_negative_amount1(self):
        order1 = BackwardTransferOrder('EUR_CASH', 'USD_CASH', (-1.0, 'USD'))
        order1.execute(self.broker_state1)
        self.assertEqual(self.broker_state1.accounts['USD_CASH'], (9999.0, 'USD'))
        self.assertEqual(self.broker_state1.accounts['EUR_CASH'],
                         (10000.0 + self.prices[('USD', 'EUR')], 'EUR'))

    def test_basic_negative_amount2(self):
        order1 = ForwardTransferOrder('EUR_CASH', 'USD_CASH', (-1.0, 'EUR'))
        order1.execute(self.broker_state1)
        self.assertEqual(self.broker_state1.accounts['EUR_CASH'], (10001.0, 'EUR'))
        self.assertEqual(self.broker_state1.accounts['USD_CASH'],
                         (10000.0 - 1.0 / self.prices[('USD', 'EUR')], 'USD'))

    def test_xau1(self):
        order1 = BackwardTransferOrder('EUR_CASH', 'USD_CASH', (1.0, 'XAU'))
        order2 = BackwardTransferOrder('USD_CASH', 'XAU_BARS', (1.0, 'XAU'))

        order1.execute(self.broker_state1)
        order2.execute(self.broker_state1)

        self.assertNotEqual(vars(self.broker_state1), vars(self.broker_state2))
        self.assertEqual(self.broker_state1.accounts['USD_CASH'], (10000.0, 'USD'))
        self.assertEqual(self.broker_state1.accounts['XAU_BARS'], (2.0, 'XAU'))

    @unittest.skip
    def test_straight_vs_inverted(self):
        order1 = BackwardTransferOrder('EUR_CASH', 'USD_CASH', (100.0, 'EUR'))
        order2 = order1.inversed()
        self.assertNotEqual(vars(order1), vars(order2))

        order1.execute(self.broker_state1)
        order2.execute(self.broker_state2)

        self.assertEqual(vars(self.broker_state1), vars(self.broker_state2))
        self.assertEqual(self.broker_state1.accounts['EUR_CASH'], (9900.0, 'EUR'))
        self.assertEqual(self.broker_state1.accounts['USD_CASH'], (10000.0 + 100*1.2*self.spread, 'USD'))
        self.assertEqual(self.broker_state1.accounts['XAU_BARS'], (1.0, 'XAU'))

    def test_three_num_backward_transfer1(self):
        order1 = BackwardTransferOrder('EUR_CASH', 'USD_CASH', (1.0, 'XAU'))
        order1.execute(self.broker_state1)

        # order2 = order1.inversed()
        # order2.execute(self.broker_state2)
        # self.assertEqual(vars(self.broker_state1), vars(self.broker_state2))
        self.assertNotEqual(self.broker_state1.accounts, self.accounts)

        self.assertEqual(self.broker_state1.accounts['EUR_CASH'],
                         (10000.0 - 1.0 / (self.prices[('USD', 'XAU')] * self.prices[('EUR', 'USD')]), 'EUR'))
        self.assertEqual(self.broker_state1.accounts['USD_CASH'],
                         (10000.0 + 1.0 / self.prices[('USD', 'XAU')], 'USD'))
        self.assertEqual(self.broker_state1.accounts['XAU_BARS'], (1.0, 'XAU'))

    def test_three_num_forward_transfer1(self):
        order1 = ForwardTransferOrder('EUR_CASH', 'USD_CASH', (1.0, 'XAU'))
        order1.execute(self.broker_state1)
        # order2 = order1.inversed()
        # order2.execute(self.broker_state2)
        # self.assertEqual(vars(self.broker_state1), vars(self.broker_state2))

        self.assertNotEqual(self.broker_state1.accounts, self.accounts)

        self.assertEqual(self.broker_state1.accounts['EUR_CASH'],
                         (10000.0 - 1.0/self.prices[('EUR', 'XAU')], 'EUR'))
        self.assertEqual(self.broker_state1.accounts['USD_CASH'],
                         (10000.0 + self.prices[('EUR', 'USD')]/self.prices[('EUR', 'XAU')], 'USD'))
        self.assertEqual(self.broker_state1.accounts['XAU_BARS'], (1.0, 'XAU'))

    def test_ping_pong_long1(self):
        """ USD_CASH gets USD to buy 1 EUR and buys it"""
        order_ping = BackwardTransferOrder('EUR_CASH', 'USD_CASH', (1.0, 'EUR'))
        order_pong = BackwardTransferOrder('USD_CASH', 'EUR_CASH', (1.0, 'EUR'))
        values0, values1 = self.do_ping_pong(order_ping, order_pong, 15)
        for i in range(1, len(values0)):
            self.assertLess(values0[i], values0[i - 1])
            self.assertEqual(values1[i], values1[i - 1])

    def test_ping_pong2(self):
        order_ping = BackwardTransferOrder('EUR_CASH', 'USD_CASH', (1.0, 'XAU'))
        order_pong = BackwardTransferOrder('USD_CASH', 'EUR_CASH', (1.0, 'XAU'))
        values0, values1 = self.do_ping_pong(order_ping, order_pong, 15)
        for i in range(1, len(values0)):
            self.assertLess(values0[i], values0[i - 1])
            self.assertLess(values1[i], values1[i - 1])

    def test_ping_pong3(self):
        order_ping = BackwardTransferOrder('EUR_CASH', 'USD_CASH', (1.0, 'USD'))
        order_pong = BackwardTransferOrder('USD_CASH', 'EUR_CASH', (1.0, 'USD'))
        values0, values1 = self.do_ping_pong(order_ping, order_pong, 15)
        for i in range(1, len(values0)):
            self.assertEqual(values0[i], values0[i - 1])
            self.assertLess(values1[i], values1[i - 1])

    def do_ping_pong(self, order_ping: BackwardTransferOrder, order_pong: BackwardTransferOrder, times: int):
        values0, values1 = [], []

        for i in range(times):
            values0.append(self.broker_state1.accounts['EUR_CASH'][0])
            values1.append(self.broker_state1.accounts['USD_CASH'][0])
            order_ping.execute(self.broker_state1)
            order_pong.execute(self.broker_state1)

        return values0, values1
