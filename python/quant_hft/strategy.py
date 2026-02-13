"""Python strategy base wrapper for quant_hft_strategy bindings."""

try:
    from quant_hft_strategy import Strategy  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    class Strategy:
        def initialize(self) -> None:
            return None

        def on_tick(self, tick) -> None:
            return None

        def on_bar(self, bar) -> None:
            return None

        def on_order(self, order) -> None:
            return None

        def on_trade(self, trade) -> None:
            return None

        def buy(self, symbol: str, price: float, volume: int) -> None:
            return None

        def sell(self, symbol: str, price: float, volume: int) -> None:
            return None

        def cancel_order(self, client_order_id: str) -> None:
            return None
