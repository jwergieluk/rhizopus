import datetime
import logging
import math
from io import StringIO
from typing import Dict, Optional, Sequence, Iterable, Tuple, List

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from rhizopus.broker_observer import BrokerObserver
from rhizopus.broker_simulator import (
    TransactionCostFilter,
    BrokerSimulator,
    SeriesStoreBase,
    SeriesStoreFromDict,
)
from rhizopus.orders import CreateAccountOrder, InterestOrder
from rhizopus.strategy import Strategy

from rhizopus.broker import Broker, Order

# Setup a logger to print messages produced by the framework
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(levelname)s: %(name)s: %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ")
handler.setFormatter(formatter)
logger.addHandler(handler)

# matplotlib setup
DPI = 150
FIG_WIDTH = 8.0
FIG_HEIGHT = 5.0
FIG_SIZE = (FIG_WIDTH, FIG_HEIGHT)
plt.style.use('ggplot')

DEFAULT_NUMERAIRE = 'EUR'

# Eurostat exchange rate data https://ec.europa.eu/eurostat/web/exchange-and-interest-rates/data/database
EUROSTAT_FX_DATA = """t,2021M09D20 ,2021M09D17 ,2021M09D16 ,2021M09D15 ,2021M09D14 ,2021M09D13 ,2021M09D10 ,2021M09D09 ,2021M09D08 ,2021M09D07 ,2021M09D06 ,2021M09D03 ,2021M09D02 ,2021M09D01 ,2021M08D31 ,2021M08D30 ,2021M08D27 ,2021M08D26 ,2021M08D25 ,2021M08D24 ,2021M08D23 ,2021M08D20 ,2021M08D19 ,2021M08D18 ,2021M08D17 ,2021M08D16 ,2021M08D13 ,2021M08D12 ,2021M08D11 ,2021M08D10 ,2021M08D09 ,2021M08D06 ,2021M08D05 ,2021M08D04 ,2021M08D03 ,2021M08D02 ,2021M07D30 ,2021M07D29 ,2021M07D28 ,2021M07D27 ,2021M07D26 ,2021M07D23 ,2021M07D22 ,2021M07D21 ,2021M07D20 ,2021M07D19 ,2021M07D16 ,2021M07D15 ,2021M07D14 ,2021M07D13 ,2021M07D12 ,2021M07D09 ,2021M07D08 ,2021M07D07 ,2021M07D06 ,2021M07D05 ,2021M07D02 ,2021M07D01 
USD,1.1711 ,1.1780 ,1.1763 ,1.1824 ,1.1814 ,1.1780 ,1.1841 ,1.1838 ,1.1827 ,1.1860 ,1.1864 ,1.1872 ,1.1846 ,1.1817 ,1.1834 ,1.1801 ,1.1761 ,1.1767 ,1.1736 ,1.1740 ,1.1718 ,1.1671 ,1.1696 ,1.1723 ,1.1767 ,1.1772 ,1.1765 ,1.1739 ,1.1718 ,1.1722 ,1.1761 ,1.1807 ,1.1850 ,1.1861 ,1.1885 ,1.1886 ,1.1891 ,1.1873 ,1.1807 ,1.1810 ,1.1787 ,1.1767 ,1.1775 ,1.1772 ,1.1775 ,1.1766 ,1.1802 ,1.1809 ,1.1812 ,1.1844 ,1.1852 ,1.1858 ,1.1838 ,1.1831 ,1.1838 ,1.1866 ,1.1823 ,1.1884  
KRW,1393.32 ,1386.26 ,1380.44 ,1380.77 ,1382.66 ,1383.74 ,1380.55 ,1382.73 ,1377.95 ,1380.15 ,1372.45 ,1374.06 ,1372.66 ,1369.25 ,1370.03 ,1375.03 ,1375.78 ,1373.36 ,1370.17 ,1369.00 ,1372.54 ,1380.66 ,1375.34 ,1371.95 ,1381.91 ,1374.09 ,1369.96 ,1365.19 ,1357.41 ,1349.76 ,1345.82 ,1349.15 ,1353.34 ,1355.44 ,1364.59 ,1367.67 ,1368.74 ,1360.07 ,1362.99 ,1360.75 ,1360.69 ,1354.19 ,1352.59 ,1359.56 ,1352.39 ,1357.53 ,1347.94 ,1350.13 ,1355.24 ,1356.76 ,1361.95 ,1358.20 ,1361.34 ,1344.89 ,1341.67 ,1341.22 ,1343.49 ,1345.86 
JPY,128.18 ,129.61 ,128.67 ,129.11 ,130.08 ,129.62 ,130.03 ,130.10 ,130.31 ,130.51 ,130.34 ,130.54 ,130.31 ,130.35 ,129.95 ,129.66 ,129.59 ,129.60 ,129.00 ,128.74 ,129.02 ,127.97 ,128.21 ,128.67 ,128.57 ,128.75 ,129.64 ,129.61 ,129.68 ,129.48 ,129.50 ,129.64 ,129.79 ,129.31 ,129.70 ,130.17 ,130.39 ,130.41 ,129.97 ,129.98 ,130.05 ,130.11 ,129.83 ,129.63 ,129.03 ,128.96 ,130.03 ,129.93 ,130.30 ,130.55 ,130.55 ,130.46 ,129.91 ,130.86 ,130.99 ,131.58 ,131.74 ,132.42 
HUF,353.97 ,351.49 ,350.19 ,348.86 ,350.05 ,349.58 ,349.88 ,350.88 ,350.14 ,348.59 ,347.03 ,348.40 ,347.85 ,348.03 ,348.80 ,348.30 ,350.87 ,349.18 ,348.76 ,349.69 ,350.13 ,350.92 ,350.98 ,350.52 ,351.15 ,351.89 ,353.03 ,353.25 ,354.84 ,352.86 ,353.87 ,352.72 ,353.95 ,354.24 ,354.59 ,356.01 ,357.20 ,358.08 ,359.72 ,359.43 ,361.65 ,359.13 ,357.87 ,360.20 ,359.45 ,359.48 ,359.73 ,359.45 ,358.08 ,356.67 ,355.38 ,355.10 ,358.57 ,355.57 ,353.40 ,351.53 ,352.10 ,351.86 
"""


def get_series_store(default_numeraire: str) -> SeriesStoreBase:
    """Packages the raw price data above in a SeriesStore object that can be used by the simulator"""

    df = pd.read_csv(StringIO(EUROSTAT_FX_DATA), header=0, index_col=0).T
    df['date'] = pd.to_datetime(df.index.str.strip(), format='%YM%mD%d', errors='coerce')
    df = df.reset_index(drop=True).set_index('date')

    # df = pd.read_csv(StringIO(PRICES_RAW), parse_dates=['Date'], header=0, index_col=0)
    data = {
        (default_numeraire, col): [(d.to_pydatetime(), v) for d, v in df[col].to_dict().items()]
        for col in df.columns
    }
    store = SeriesStoreFromDict(data)
    store.add_inverse_series()
    return store


class ConstantMixStrategy(Strategy):
    """Reallocates a portfolio to a fixed set of weights

    The weights are specified as a dictionary. Example:

        {'SPY': 0.7, 'QQQ': 0.3}

    """

    def __init__(self, broker: Broker, target_alloc: Dict[str, float]):
        super().__init__(broker, max_rel_alloc_deviation=0.01)
        self.target_alloc = target_alloc

    def get_target_allocation(self) -> Dict[str, float]:
        return self.target_alloc


def main():
    target_alloc = {
        'USD': 0.4,
        'KRW': 0.15,
        'JPY': 0.25,
        'HUF': 0.2,
    }
    assert abs(sum(target_alloc.values()) - 1.0) < 1e-8

    series_store = get_series_store('EUR')
    filters = [
        TransactionCostFilter('EUR', 5.0, "transaction_cost", []),  # 5 EUR per transaction
    ]
    broker_simulator = BrokerSimulator(
        series_store,
        filters,
        default_numeraire='EUR',
    )
    accounts = {num: (0.0, num) for num in series_store.vertices()}
    accounts['EUR'] = (1.0e6, 'EUR')  # start capital
    initial_orders: List[Order] = [
        CreateAccountOrder(num, amount) for num, amount in accounts.items()
    ]
    initial_orders.extend(
        [
            # earn 50bps positive cash account value
            InterestOrder(
                'EUR', interest_rate=0.005, value_lower_bound=0.0, value_upper_bound=math.inf
            ),
            # pay 300bps lending cost on negative cash account value
            InterestOrder(
                'EUR', interest_rate=0.03, value_lower_bound=-math.inf, value_upper_bound=0.0
            ),
        ]
    )
    broker = Broker(broker_simulator, initial_orders=initial_orders)

    strategy = ConstantMixStrategy(broker, target_alloc)
    # On the first day we just observe the market prices and do nothing. Trading starts on the next day.
    trading_start_time = series_store.get_min_time() + datetime.timedelta(days=1)
    strategy.run(trading_start_time, max_iterations=100)

    df = get_observer_df(strategy.observer)
    plot_normalized_asset_performance(df, target_alloc.keys(), 'EUR')
    plot_account_weights(df, target_alloc.keys())


def get_observer_df(observer: BrokerObserver, keys: Optional[Sequence[str]] = None) -> pd.DataFrame:
    """Extracts observer data into a pandas DataFrame"""
    ss = []
    for key in keys or observer.keys():
        data = observer.get_history(key)
        key_name = key if isinstance(key, str) else '_'.join(key)
        s = pd.DataFrame.from_records(data=data, columns=['DateTime', key_name], index='DateTime')
        ss.append(s)
    return pd.concat(ss, axis=1, join='outer')


def plot_normalized_asset_performance(
    obs_df: pd.DataFrame,
    asset_names: Iterable[str],
    default_numeraire: str = 'EUR',
) -> None:
    cols = [f'{a}_{default_numeraire}' for a in asset_names] + ['portfolio_nav']

    obs_df = obs_df.loc[:, cols].copy()
    obs_df = obs_df / obs_df.iloc[0, :]

    obs_df.plot(figsize=FIG_SIZE, grid=True, lw=4)
    plt.tight_layout()
    plt.savefig('rhizopus_perf.png', dpi=DPI)
    plt.close('all')


def plot_account_weights(
    obs_df: pd.DataFrame,
    asset_names: Iterable[str],
):
    years = int((max(obs_df.index) - min(obs_df.index)).days / 365.0)
    cols = [f'account_{asset}_weight' for asset in asset_names]

    fig, ax = plt.subplots(1, 1, figsize=(max(8, 6 * years), 5))
    df_: pd.DataFrame = obs_df.loc[:, cols] / np.sum(
        obs_df.loc[:, cols].values, axis=1, keepdims=True
    )
    df_.plot(ax=ax, grid=True, lw=4)
    plt.tight_layout()
    plt.savefig('rhizopus_alloc_weights_rel.png', dpi=DPI)
    plt.close('all')


if __name__ == '__main__':
    main()
