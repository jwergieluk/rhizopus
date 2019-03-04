import random
import datetime
import unittest
from . import add_inverse_series, BrokerSimulator, CreateAccountOrder, Broker, BackwardTransferOrder, \
    TransactionCostFilter


class BrokerSimulatorTest(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.start_time = datetime.datetime(2000, 1, 1)
        self.series_store = {('EUR', 'USD'): None, ('USD', 'JPY'): None}
        for key in self.series_store.keys():
            self.series_store[key] = [(self.start_time + datetime.timedelta(days=t), random.gammavariate(4.0, 1.0))
                                      for t in range(20)]
        add_inverse_series(self.series_store)

    def tearDown(self):
        super().tearDown()

    def test_transfer_order1(self):
        start_time = datetime.datetime(2000, 1, 1)
        series1 = [(start_time + datetime.timedelta(days=t), 10.0) for t in range(10)]
        series2 = [(start_time + datetime.timedelta(days=t), 20.0) for t in range(10)]
        series_store = {('EUR', 'USD'): series1, ('USD', 'JPY'): series2}
        add_inverse_series(series_store)

        market = BrokerSimulator(series_store, filters=[], default_numeraire='EUR',
                                 start_time=start_time)

        accounts = {'EUR_CASH': (100.0, 'EUR'), 'JPY_CASH': (0.0, 'JPY'), 'USD_CASH': (0.0, 'USD')}
        orders = [CreateAccountOrder(acc, amount) for acc, amount in accounts.items()]
        broker = Broker(market, initial_orders=orders)

        # fill an order that cannot be executed
        broker.fill_order(BackwardTransferOrder('EUR_CASH', 'JPY_CASH', (100, 'EUR')))
        broker.fill_order(BackwardTransferOrder('EUR_CASH', 'USD_CASH', (10, 'EUR')))

        while broker.next() is not None:
            pass

        accounts = broker.get_accounts()
        self.assertEqual(len(accounts.keys()), 3)
        self.assertEqual(accounts['EUR_CASH'], (90.0, 'EUR'))
        self.assertEqual(accounts['USD_CASH'], (100.0, 'USD'))
        self.assertEqual(accounts['JPY_CASH'], (0.0, 'JPY'))

    def test_transfer_order2(self):
        """Prices are constant; spread is zero. Transfer at random between the cash accounts and test
         whether the value of all accounts stays constant.
        """
        start_time = datetime.datetime(2000, 1, 1)
        series_store = {('EUR', 'USD'): None, ('USD', 'JPY'): None}
        for key in series_store.keys():
            const_price = random.choice([2, 4, 8, 10])
            series_store[key] = [(start_time + datetime.timedelta(days=t), const_price) for t in range(20)]
        add_inverse_series(series_store)
        market = BrokerSimulator(series_store, filters=[], default_numeraire='EUR',
                                 start_time=start_time)
        accounts = {'EUR_CASH': (100.0, 'EUR'), 'JPY_CASH': (0.0, 'JPY'), 'USD_CASH': (0.0, 'USD')}

        orders = [CreateAccountOrder(acc, amount) for acc, amount in accounts.items()]
        broker = Broker(market, initial_orders=orders)

        while broker.next() is not None:
            self.assertAlmostEqual(1.0, sum(broker.get_weight_all_accounts().values()))
            self.assertAlmostEqual(100.0, broker.get_value_portfolio('EUR'))
            self.assertAlmostEqual(100.0, sum(broker.get_value_all_accounts('EUR').values()))
            broker.fill_order(BackwardTransferOrder('EUR_CASH', 'JPY_CASH', (random.gammavariate(10.0, 1.0), 'JPY')))
            broker.fill_order(BackwardTransferOrder('JPY_CASH', 'USD_CASH', (random.gammavariate(10.0, 1.0), 'USD')))
            broker.fill_order(BackwardTransferOrder('USD_CASH', 'EUR_CASH', (random.gammavariate(10.0, 1.0), 'USD')))

    def test_transaction_cost_filter(self):
        accounts = {'EUR_CASH': (100.0, 'EUR'), 'JPY_CASH': (0.0, 'JPY'), 'USD_CASH': (0.0, 'USD')}
        orders = [CreateAccountOrder(acc, amount) for acc, amount in accounts.items()]
        filters = [TransactionCostFilter('EUR_CASH', 5, 'tc', ['EUR_CASH', 'USD_CASH'])]
        market = BrokerSimulator(self.series_store, filters, default_numeraire='EUR')
        broker = Broker(market, initial_orders=orders)

        broker.fill_order(BackwardTransferOrder('EUR_CASH', 'JPY_CASH', (5.0, 'EUR')))
        broker.fill_order(BackwardTransferOrder('EUR_CASH', 'JPY_CASH', (5.0, 'EUR')))
        broker.fill_order(BackwardTransferOrder('EUR_CASH', 'USD_CASH', (5.0, 'EUR')))
        broker.next()

        self.assertTrue('tc' in broker.get_variables().keys())
        self.assertAlmostEqual(broker.get_variables()['tc'], 10.0)


class TestBrokerAccountValuation(unittest.TestCase):
    def test_observer1(self):
        start_time = datetime.datetime(2000, 1, 1)
        eur_usd_series = [(start_time + datetime.timedelta(days=t), 2.0) for t in range(3)]
        spx_usd_series = [(start_time + datetime.timedelta(days=t), 2000) for t in range(3)]
        series_store = {('EUR', 'USD'): eur_usd_series, ('SPX', 'USD'): spx_usd_series}
        add_inverse_series(series_store)
        market = BrokerSimulator(series_store, filters=[],
                                 default_numeraire='EUR', start_time=start_time)

        accounts = {'EUR_CASH': (1000.0, 'EUR'), 'USD_CASH': (0.0, 'USD'), 'SPX': (0.0, 'SPX')}
        orders = [CreateAccountOrder(acc, amount) for acc, amount in accounts.items()]
        broker = Broker(market, initial_orders=orders)

        broker.fill_order(BackwardTransferOrder('EUR_CASH', 'USD_CASH', (1000.0, 'EUR')))
        broker.fill_order(BackwardTransferOrder('USD_CASH', 'SPX', (1.0, 'SPX')))
        while broker.next() is not None:
            pass

        account_values_eur = broker.get_value_all_accounts('EUR')
        accounts = broker.get_accounts()

        self.assertEqual(accounts['EUR_CASH'], (0.0, 'EUR'))
        self.assertEqual(accounts['USD_CASH'], (0.0, 'USD'))
        self.assertEqual(accounts['SPX'], (1.0, 'SPX'))

        self.assertEqual(account_values_eur['EUR_CASH'], 0.0)
        self.assertEqual(account_values_eur['USD_CASH'], 0.0)
        self.assertEqual(account_values_eur['SPX'], 1000.0)

        self.assertEqual(broker.get_value_all_accounts('USD')['SPX'], 2000.0)
