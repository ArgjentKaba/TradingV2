class PaperExec:
    def __init__(
        self,
        symbol: str,
        profile: str = "SAFE",
        start_equity: float = 10000.0,
        risk_override: float | None = None,
        sl_pct: float = 6.0,
        tp1_pct: float = 8.0,
        tp2_pct: float = 12.0,
    ):
        self.symbol = symbol
        self.profile = profile.upper()
        self.equity = start_equity
        self.sl_pct = sl_pct
        self.tp1_pct = tp1_pct
        self.tp2_pct = tp2_pct
        self.risk_map = {"SAFE": 0.005, "FAST": 0.010}
        self.risk_override = risk_override
        self.trades = []

    def _current_risk(self) -> float:
        return float(
            self.risk_override
            if self.risk_override is not None
            else self.risk_map.get(self.profile, 0.005)
        )

    def execute_trade(
        self,
        side: str,
        entry_price: float,
        exit_price: float,
        time_entry,
        time_exit,
        reason: str,
        time_limit_applied: bool = False,
        unrealized_pct_at_90m: float | None = None,
        be_armed: bool = False,
    ):
        equity_before = self.equity
        risk_perc = self._current_risk()
        notional_total = equity_before * risk_perc / (self.sl_pct / 100.0)
        qty_total = notional_total / entry_price

        reason_lower = reason.lower()
        if "tp1" in reason_lower:
            frac = 0.33
            leg = "TP1"
        elif "tp2" in reason_lower:
            frac = 0.67
            leg = "TP2"
        elif "stopbe" in reason_lower or "timemax_90m_be" in reason_lower:
            frac = 0.67 if "stopbe" in reason_lower else 1.0
            leg = "BE" if "stopbe" in reason_lower else "FULL"
        else:
            frac = 1.0
            leg = "FULL"

        notional = notional_total * frac
        qty = qty_total * frac

        pnl_pct_price = (exit_price / entry_price - 1.0) * 100.0
        R_multiple = pnl_pct_price / self.sl_pct if self.sl_pct else 0.0

        account_pnl_usd = notional * (exit_price / entry_price - 1.0)
        account_pnl_pct = (
            (account_pnl_usd / equity_before) * 100.0 if equity_before else 0.0
        )
        equity_after = equity_before + account_pnl_usd
        self.equity = equity_after

        row = {
            "time_entry": str(time_entry),
            "time_exit": str(time_exit),
            "symbol": self.symbol,
            "side": side,
            "entry": f"{entry_price:.6f}",
            "exit": f"{exit_price:.6f}",
            "pnl_pct": f"{pnl_pct_price:.4f}",
            "reason": reason,
            "profile_run": self.profile,
            "risk_perc_run": f"{risk_perc*100:.2f}",
            "R_multiple": f"{R_multiple:.4f}",
            "account_pnl_pct": f"{account_pnl_pct:.4f}",
            "account_pnl_usd": f"{account_pnl_usd:.2f}",
            "equity_before": f"{equity_before:.2f}",
            "equity_after": f"{equity_after:.2f}",
            "qty": f"{qty:.6f}",
            "notional_usd": f"{notional:.2f}",
            "time_limit_applied": str(bool(time_limit_applied)),
            "time_limit_minutes": "90",
            "unrealized_pct_at_90m": (
                f"{unrealized_pct_at_90m:.4f}"
                if unrealized_pct_at_90m is not None
                else ""
            ),
            "be_armed": str(bool(be_armed)),
            "leg": leg,
            "leg_fraction": f"{frac:.2f}",
        }
        self.trades.append(row)
