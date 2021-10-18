import random
import datetime

import pytest

from rhizopus.broker import Broker
from rhizopus.broker_simulator import BrokerSimulator, TransactionCostFilter, SeriesStoreFromDict
from rhizopus.orders import CreateAccountOrder, BackwardTransferOrder


def test_transfer_order1():
    start_time = datetime.datetime(2000, 1, 1)
    series1 = [(start_time + datetime.timedelta(days=t), 10.0) for t in range(10)]
    series2 = [(start_time + datetime.timedelta(days=t), 20.0) for t in range(10)]
    series_store = SeriesStoreFromDict({('EUR', 'USD'): series1, ('USD', 'JPY'): series2})
    series_store.add_inverse_series()

    market = BrokerSimulator(
        series_store, filters=[], default_numeraire='EUR', start_time=start_time
    )

    accounts = {
        'EUR_CASH': (100.0, 'EUR'),
        'JPY_CASH': (0.0, 'JPY'),
        'USD_CASH': (0.0, 'USD'),
    }
    orders = [CreateAccountOrder(acc, amount) for acc, amount in accounts.items()]
    broker = Broker(market, initial_orders=orders)

    # fill an order that cannot be executed
    broker.fill_order(BackwardTransferOrder('EUR_CASH', 'JPY_CASH', (100.0, 'EUR')))
    broker.fill_order(BackwardTransferOrder('EUR_CASH', 'USD_CASH', (10.0, 'EUR')))

    while broker.next() is not None:
        pass

    accounts = broker.get_accounts()
    assert len(accounts) == 3
    assert accounts['EUR_CASH'] == (90.0, 'EUR')
    assert accounts['USD_CASH'] == (100.0, 'USD')
    assert accounts['JPY_CASH'] == (0.0, 'JPY')


def test_transfer_order2():
    """Prices are constant; spread is zero. Transfer at random between the cash accounts and test
    whether the value of all accounts stays constant.
    """
    start_time = datetime.datetime(2000, 1, 1)
    series = {('EUR', 'USD'): None, ('USD', 'JPY'): None}
    for key in series:
        const_price = random.choice([2, 4, 8, 10])
        series[key] = [(start_time + datetime.timedelta(days=t), const_price) for t in range(20)]
    series_store = SeriesStoreFromDict(series)
    series_store.add_inverse_series()
    market = BrokerSimulator(
        series_store, filters=[], default_numeraire='EUR', start_time=start_time
    )
    accounts = {
        'EUR_CASH': (100.0, 'EUR'),
        'JPY_CASH': (0.0, 'JPY'),
        'USD_CASH': (0.0, 'USD'),
    }

    orders = [CreateAccountOrder(acc, amount) for acc, amount in accounts.items()]
    broker = Broker(market, initial_orders=orders)

    while broker.next() is not None:
        assert 1.0 == pytest.approx(sum(broker.get_weight_all_accounts().values()))
        assert 100.0 == pytest.approx(broker.get_value_portfolio('EUR'))
        assert 100.0 == pytest.approx(sum(broker.get_value_all_accounts('EUR').values()))
        broker.fill_order(
            BackwardTransferOrder('EUR_CASH', 'JPY_CASH', (random.gammavariate(10.0, 1.0), 'JPY'))
        )
        broker.fill_order(
            BackwardTransferOrder('JPY_CASH', 'USD_CASH', (random.gammavariate(10.0, 1.0), 'USD'))
        )
        broker.fill_order(
            BackwardTransferOrder('USD_CASH', 'EUR_CASH', (random.gammavariate(10.0, 1.0), 'USD'))
        )


def test_transaction_cost_filter():
    start_time = datetime.datetime(2000, 1, 1)
    series = {('EUR', 'USD'): None, ('USD', 'JPY'): None}
    for key in series:
        series[key] = [
            (
                start_time + datetime.timedelta(days=t),
                random.gammavariate(4.0, 1.0),
            )
            for t in range(20)
        ]
    series_store = SeriesStoreFromDict(series)
    series_store.add_inverse_series()

    accounts = {
        'EUR_CASH': (100.0, 'EUR'),
        'JPY_CASH': (0.0, 'JPY'),
        'USD_CASH': (0.0, 'USD'),
    }
    orders = [CreateAccountOrder(acc, amount) for acc, amount in accounts.items()]
    filters = [TransactionCostFilter('EUR_CASH', 5.0, 'tc', ['EUR_CASH', 'USD_CASH'])]
    market = BrokerSimulator(series_store, filters, default_numeraire='EUR')
    broker = Broker(market, initial_orders=orders)

    broker.fill_order(BackwardTransferOrder('EUR_CASH', 'JPY_CASH', (5.0, 'EUR')))
    broker.fill_order(BackwardTransferOrder('EUR_CASH', 'JPY_CASH', (5.0, 'EUR')))
    broker.fill_order(BackwardTransferOrder('EUR_CASH', 'USD_CASH', (5.0, 'EUR')))
    broker.next()

    assert 'tc' in broker.get_variables()
    assert broker.get_variables()['tc'] == 10.0


def test_observer1():
    start_time = datetime.datetime(2000, 1, 1)
    eur_usd_series = [(start_time + datetime.timedelta(days=t), 2.0) for t in range(3)]
    spx_usd_series = [(start_time + datetime.timedelta(days=t), 2000) for t in range(3)]
    series = {('EUR', 'USD'): eur_usd_series, ('SPX', 'USD'): spx_usd_series}
    series_store = SeriesStoreFromDict(series)
    series_store.add_inverse_series()
    market = BrokerSimulator(
        series_store, filters=[], default_numeraire='EUR', start_time=start_time
    )

    accounts = {
        'EUR_CASH': (1000.0, 'EUR'),
        'USD_CASH': (0.0, 'USD'),
        'SPX': (0.0, 'SPX'),
    }
    orders = [CreateAccountOrder(acc, amount) for acc, amount in accounts.items()]
    broker = Broker(market, initial_orders=orders)

    broker.fill_order(BackwardTransferOrder('EUR_CASH', 'USD_CASH', (1000.0, 'EUR')))
    broker.fill_order(BackwardTransferOrder('USD_CASH', 'SPX', (1.0, 'SPX')))
    while broker.next() is not None:
        pass

    account_values_eur = broker.get_value_all_accounts('EUR')
    accounts = broker.get_accounts()

    assert accounts['EUR_CASH'] == (0.0, 'EUR')
    assert accounts['USD_CASH'] == (0.0, 'USD')
    assert accounts['SPX'] == (1.0, 'SPX')

    assert account_values_eur['EUR_CASH'] == 0.0
    assert account_values_eur['USD_CASH'] == 0.0
    assert account_values_eur['SPX'] == 1000.0

    assert broker.get_value_all_accounts('USD')['SPX'] == 2000.0
