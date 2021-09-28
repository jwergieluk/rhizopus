import logging
import datetime
from collections import defaultdict
from typing import Optional, Dict, MutableMapping
from rhizopus.broker import Broker
from rhizopus.broker_observer import BrokerObserver
from rhizopus.orders import BackwardTransferOrder

logger = logging.getLogger(__name__)


class Strategy:
    """A sample trading strategy class that executes the strategy loop"""

    def __init__(
        self,
        broker: Broker,
        max_rel_alloc_deviation: float = 0.01,
    ):
        self.broker = broker
        self.default_numeraire = self.broker.get_default_numeraire()
        self.asset_numeraires = [
            k for k in self.broker.get_accounts() if k != self.default_numeraire
        ]
        self.current_portfolio_weights: Optional[Dict[str, float]] = None

        # Total relative (weight) deviation from the target allocation smaller than the following param will not
        # trigger a reallocation.
        self.max_rel_alloc_deviation = max_rel_alloc_deviation

        self.observer = BrokerObserver(broker)
        self.price_cache = {}

    def run(self, start_time: datetime.datetime, max_iterations: int):
        """Executes the strategy loop"""
        while self.broker.get_time() < start_time:
            self.observer.update()
            self.broker.next()
        for time_index in range(max_iterations):
            self.observer.update()
            new_orders = self._get_orders()
            for order in new_orders:
                self.broker.fill_order(order)
            if self.broker.next() is None:
                break

    def get_target_allocation(self) -> Optional[MutableMapping[str, float]]:
        """Portfolio allocation method

        Override this method to implement your strategy
        """
        raise NotImplementedError

    def _get_orders(self):
        if len(self.broker.get_active_orders()) > 0:
            logger.info(
                f"Skip generate_orders() for time {self.observer.now}: Active orders found."
            )
            return []
        self._update_price_cache()
        self.current_portfolio_weights = {
            k: v
            for k, v in self.broker.get_weight_all_accounts().items()
            if k != self.default_numeraire
        }
        target_asset_allocation = self.get_target_allocation()
        if target_asset_allocation is None:
            logger.info(
                f"Skip generate_orders() for time {self.observer.now}: No target allocation calculated."
            )
            self.observer.save(("portfolio", "reallocation_mass"), 0.0)
            return []
        return self._get_orders_for_allocation(target_asset_allocation)

    def _update_price_cache(self):
        recent_observations = self.observer.get_recent_observations()
        for acc in self.broker.get_accounts():
            if acc == self.default_numeraire:
                continue
            key = (acc, self.default_numeraire)
            if key in recent_observations:
                self.price_cache[acc] = recent_observations[key]

        key = ("portfolio", "nav")
        if key in recent_observations:
            self.price_cache["portfolio"] = recent_observations[key]

    def _get_orders_for_allocation(self, target_weights: MutableMapping[str, float]):
        """Converts a relative target allocation into a bunch of orders"""
        assert self.default_numeraire not in target_weights
        target_weights = defaultdict(float, target_weights)
        accounts = {
            k: v for k, v in self.broker.get_accounts().items() if k != self.default_numeraire
        }
        weights = self.broker.get_weight_all_accounts()
        nav = self.broker.get_value_portfolio(self.default_numeraire)
        cash_acc = self.default_numeraire  # cash account name is the default numeraire name

        if self.broker.get_value_portfolio() is None:
            logger.warning(f"Portfolio value is not well-defined for time {self.observer.now}")
            return []

        orders = []

        reallocation_mass = sum(abs(weights[acc] - target_weights[acc]) for acc in accounts)
        if reallocation_mass < self.max_rel_alloc_deviation:
            logger.info(
                f"Reallocation mass of {reallocation_mass:0.4f} is below "
                f"the threshold of {self.max_rel_alloc_deviation:0.4f}: No rebalancing."
            )
            self.observer.save(("portfolio", "reallocation_mass"), 0.0)
            self.observer.save(("portfolio", "turnover_rate"), 0.0)
            return orders
        self.observer.save(("portfolio", "reallocation_mass"), reallocation_mass)
        logger.info(f"Reallocation mass: {reallocation_mass:.4f}")
        for asset_acc in accounts:
            asset_num = accounts[asset_acc][1]
            amount_value = (target_weights[asset_num] - weights[asset_acc]) * nav
            if abs(amount_value) < 0.01:  # don't trade below 0.01 DEFAULT_NUMERAIRE
                continue
            amount_num = self.default_numeraire
            amount = (amount_value, amount_num)
            orders.append(BackwardTransferOrder(cash_acc, asset_acc, amount))
        return orders
