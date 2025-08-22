from datetime import datetime, timedelta


class Governor:
    def __init__(
        self,
        profile: str = "SAFE",
        trades_min_per_day: int = 2,
        trades_max_per_day: int = 4,
        cooldown_minutes: int = 30,
    ):
        self.profile = profile.upper()
        self.trades_min = trades_min_per_day
        self.trades_max = trades_max_per_day
        self.cooldown = timedelta(minutes=cooldown_minutes)
        self._count_by_day = {}
        self._last_exit_time_by_symbol = {}

    def _day_key(self, t: datetime) -> str:
        return t.strftime("%Y-%m-%d")

    def can_trade(self, t: datetime, symbol: str) -> bool:
        k = self._day_key(t)
        count = self._count_by_day.get(k, 0)
        if count >= self.trades_max:
            return False
        last = self._last_exit_time_by_symbol.get(symbol)
        if last is not None and t - last < self.cooldown:
            return False
        return True

    def register_trade(self, t: datetime):
        k = self._day_key(t)
        self._count_by_day[k] = self._count_by_day.get(k, 0) + 1

    def register_exit(self, t: datetime, symbol: str):
        self._last_exit_time_by_symbol[symbol] = t
