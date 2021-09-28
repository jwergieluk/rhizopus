import pytest

from rhizopus.broker import BrokerState
from rhizopus.orders import BackwardTransferOrder, ForwardTransferOrder


@pytest.fixture()
def sample_prices():
    spread = 0.95
    prices = {
        ('EUR', 'USD'): spread * 1.2,
        ('USD', 'EUR'): spread * 1.0 / 1.2,
        ('XAU', 'EUR'): spread * 1000.0,
        ('EUR', 'XAU'): spread * 1.0 / 1000.0,
        ('XAU', 'USD'): spread * 1000.0 * 1.2,
        ('USD', 'XAU'): spread * 1.0 / (1000.0 * 1.2),
    }
    return prices


@pytest.fixture(scope='function')
def samples_accounts():
    accounts = {
        'EUR_CASH': (10000.0, 'EUR'),
        'USD_CASH': (10000.0, 'USD'),
        'XAU_BARS': (1.0, 'XAU'),
    }
    return accounts


@pytest.fixture(scope='function')
def gen_sample_broker_state(sample_prices, samples_accounts):
    def sample_broker_state():
        broker_state = BrokerState('EUR', samples_accounts)
        broker_state.current_prices.update(sample_prices)
        return broker_state

    return sample_broker_state


def test_one_num_transfer():
    accounts = {'CASH1': (10.0, 'EUR'), 'CASH2': (-20.0, 'EUR')}
    broker_state1 = BrokerState('XAU', accounts)
    # broker_state2 = BrokerState('XAU', accounts)

    order1 = BackwardTransferOrder('CASH1', 'CASH2', (1.0, 'EUR'))
    # order2 = order1.inversed()

    order1.execute(broker_state1)
    # order2.execute(broker_state2)

    # self.assertEqual(vars(broker_state1), vars(broker_state2))
    assert broker_state1.accounts['CASH1'] == (9.0, 'EUR')
    assert broker_state1.accounts['CASH2'] == (-19.0, 'EUR')


def test_price_noarb(sample_prices):
    values = []
    capital = 100.0
    for i in range(10):
        capital = capital * sample_prices[('EUR', 'USD')] * sample_prices[('USD', 'EUR')]
        values.append(capital)

    for i in range(1, len(values)):
        assert values[i] < values[i - 1]


def test_basic1(sample_prices, gen_sample_broker_state):
    broker_state1 = gen_sample_broker_state()
    order1 = BackwardTransferOrder('EUR_CASH', 'USD_CASH', (1.0, 'USD'))
    order1.execute(broker_state1)
    assert broker_state1.accounts['USD_CASH'] == (10001.0, 'USD')
    assert broker_state1.accounts['EUR_CASH'] == (
        10000.0 - 1.0 / sample_prices[('EUR', 'USD')],
        'EUR',
    )


def test_basic2(sample_prices, gen_sample_broker_state):
    broker_state1 = gen_sample_broker_state()
    order1 = ForwardTransferOrder('EUR_CASH', 'USD_CASH', (1.0, 'EUR'))
    order1.execute(broker_state1)
    assert broker_state1.accounts['EUR_CASH'] == (9999.0, 'EUR')
    assert broker_state1.accounts['USD_CASH'] == (10000.0 + sample_prices[('EUR', 'USD')], 'USD')


def test_basic_negative_amount1(sample_prices, gen_sample_broker_state):
    broker_state1 = gen_sample_broker_state()
    order1 = BackwardTransferOrder('EUR_CASH', 'USD_CASH', (-1.0, 'USD'))
    order1.execute(broker_state1)
    assert broker_state1.accounts['USD_CASH'], (9999.0, 'USD')
    assert broker_state1.accounts['EUR_CASH'] == (10000.0 + sample_prices[('USD', 'EUR')], 'EUR')


def test_basic_negative_amount2(sample_prices, gen_sample_broker_state):
    broker_state1 = gen_sample_broker_state()
    order1 = ForwardTransferOrder('EUR_CASH', 'USD_CASH', (-1.0, 'EUR'))
    order1.execute(broker_state1)
    assert broker_state1.accounts['EUR_CASH'] == (10001.0, 'EUR')
    assert broker_state1.accounts['USD_CASH'] == (
        10000.0 - 1.0 / sample_prices[('USD', 'EUR')],
        'USD',
    )


def test_xau1(gen_sample_broker_state):
    broker_state1 = gen_sample_broker_state()
    broker_state2 = gen_sample_broker_state()
    order1 = BackwardTransferOrder('EUR_CASH', 'USD_CASH', (1.0, 'XAU'))
    order2 = BackwardTransferOrder('USD_CASH', 'XAU_BARS', (1.0, 'XAU'))

    order1.execute(broker_state1)
    order2.execute(broker_state1)

    assert vars(broker_state1) != vars(broker_state2)
    assert broker_state1.accounts['USD_CASH'] == (10000.0, 'USD')
    assert broker_state1.accounts['XAU_BARS'] == (2.0, 'XAU')


def test_three_num_backward_transfer1(samples_accounts, sample_prices, gen_sample_broker_state):
    broker_state1 = gen_sample_broker_state()
    order1 = BackwardTransferOrder('EUR_CASH', 'USD_CASH', (1.0, 'XAU'))
    order1.execute(broker_state1)

    # order2 = order1.inversed()
    # order2.execute(self.broker_state2)
    # self.assertEqual(vars(self.broker_state1), vars(self.broker_state2))
    assert broker_state1.accounts != samples_accounts

    assert broker_state1.accounts['EUR_CASH'] == (
        10000.0 - 1.0 / (sample_prices[('USD', 'XAU')] * sample_prices[('EUR', 'USD')]),
        'EUR',
    )
    assert broker_state1.accounts['USD_CASH'] == (
        10000.0 + 1.0 / sample_prices[('USD', 'XAU')],
        'USD',
    )
    assert broker_state1.accounts['XAU_BARS'] == (1.0, 'XAU')


def test_three_num_forward_transfer1(samples_accounts, sample_prices, gen_sample_broker_state):
    broker_state1 = gen_sample_broker_state()
    order1 = ForwardTransferOrder('EUR_CASH', 'USD_CASH', (1.0, 'XAU'))
    order1.execute(broker_state1)
    # order2 = order1.inversed()
    # order2.execute(self.broker_state2)
    # self.assertEqual(vars(self.broker_state1), vars(self.broker_state2))

    assert broker_state1.accounts != samples_accounts
    assert broker_state1.accounts['EUR_CASH'] == (
        10000.0 - 1.0 / sample_prices[('EUR', 'XAU')],
        'EUR',
    )
    assert broker_state1.accounts['USD_CASH'] == (
        10000.0 + sample_prices[('EUR', 'USD')] / sample_prices[('EUR', 'XAU')],
        'USD',
    )
    assert broker_state1.accounts['XAU_BARS'] == (1.0, 'XAU')


def test_ping_pong_long1(gen_sample_broker_state):
    """USD_CASH gets USD to buy 1 EUR and buys it"""
    broker_state1 = gen_sample_broker_state()
    order_ping = BackwardTransferOrder('EUR_CASH', 'USD_CASH', (1.0, 'EUR'))
    order_pong = BackwardTransferOrder('USD_CASH', 'EUR_CASH', (1.0, 'EUR'))
    values0, values1 = do_ping_pong(broker_state1, order_ping, order_pong, 15)
    for i in range(1, len(values0)):
        assert values0[i] < values0[i - 1]
        assert values1[i] == values1[i - 1]


def test_ping_pong2(gen_sample_broker_state):
    broker_state1 = gen_sample_broker_state()
    order_ping = BackwardTransferOrder('EUR_CASH', 'USD_CASH', (1.0, 'XAU'))
    order_pong = BackwardTransferOrder('USD_CASH', 'EUR_CASH', (1.0, 'XAU'))
    values0, values1 = do_ping_pong(broker_state1, order_ping, order_pong, 15)
    for i in range(1, len(values0)):
        assert values0[i] < values0[i - 1]
        assert values1[i] < values1[i - 1]


def test_ping_pong3(gen_sample_broker_state):
    broker_state1 = gen_sample_broker_state()
    order_ping = BackwardTransferOrder('EUR_CASH', 'USD_CASH', (1.0, 'USD'))
    order_pong = BackwardTransferOrder('USD_CASH', 'EUR_CASH', (1.0, 'USD'))
    values0, values1 = do_ping_pong(broker_state1, order_ping, order_pong, 15)
    for i in range(1, len(values0)):
        assert values0[i] == values0[i - 1]
        assert values1[i] < values1[i - 1]


def do_ping_pong(
    broker_state1: BrokerState,
    order_ping: BackwardTransferOrder,
    order_pong: BackwardTransferOrder,
    times: int,
):
    values0, values1 = [], []

    for i in range(times):
        values0.append(broker_state1.accounts['EUR_CASH'][0])
        values1.append(broker_state1.accounts['USD_CASH'][0])
        order_ping.execute(broker_state1)
        order_pong.execute(broker_state1)

    return values0, values1
