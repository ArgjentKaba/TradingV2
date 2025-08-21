from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from collections import deque

@dataclass
class FilterThresholds:
    oi_long_min: Optional[float] = None
    oi_short_max: Optional[float] = None
    vol_z_min: float = 0.0
    mom_long_min: float = 0.0
    mom_short_max: float = 0.0
    allow_divergence: bool = False
    div_min_abs_oi: Optional[float] = None
    div_max_abs_mom: Optional[float] = None
    atr14_pct_max: float = 100.0
    liq_percentile_min: float = 0.0
    spread_bps_max: float = 1e9

class RollingStats:

    def __init__(self, vol_window:int=20):
        self.vol_window = vol_window
        self.vols = deque(maxlen=vol_window)
        self.prev_close = None
        self.trs = deque(maxlen=14)
        self.prev_high = None
        self.prev_low = None
        self.prev_close_for_tr = None

    def update(self, high: float, low: float, close: float, volume: float):
        self.vols.append(volume)
        if self.prev_close_for_tr is None:
            tr = high - low
        else:
            tr = max(high - low, abs(high - self.prev_close_for_tr), abs(low - self.prev_close_for_tr))
        self.trs.append(tr)
        self.prev_high, self.prev_low, self.prev_close_for_tr = high, low, close
        self.prev_close = close

    def vol_zscore(self) -> float:
        if len(self.vols) < 2:
            return 0.0
        import statistics as st
        m = st.mean(self.vols)
        sd = st.pstdev(self.vols) or 1e-9
        return (self.vols[-1] - m) / sd

    def atr14_pct(self) -> float:
        if len(self.trs) < 14:
            return 0.0
        atr = sum(self.trs) / len(self.trs)
        pc = self.prev_close or 1.0
        return (atr / pc) * 100.0

def sign(x: float) -> int:
    return 0 if x==0 else (1 if x>0 else -1)

def passes_gate(side:str, mom_1m_pct: float, vol_z: float, atr_pct: float,
                oi_delta_5m_pct: Optional[float], thresholds: FilterThresholds) -> bool:
    if side == "LONG":
        if mom_1m_pct < thresholds.mom_long_min:
            return False
        if thresholds.oi_long_min is not None and oi_delta_5m_pct is not None:
            if oi_delta_5m_pct < thresholds.oi_long_min:
                return False
    else:
        if mom_1m_pct > thresholds.mom_short_max:
            return False
        if thresholds.oi_short_max is not None and oi_delta_5m_pct is not None:
            if oi_delta_5m_pct > thresholds.oi_short_max:
                return False
    if vol_z < thresholds.vol_z_min:
        return False
    if atr_pct > thresholds.atr14_pct_max:
        return False
    return True

def divergence_ok(mom_1m_pct: float, oi_delta_5m_pct: Optional[float], thresholds: FilterThresholds) -> bool:
    if thresholds.allow_divergence:
        if oi_delta_5m_pct is None:
            return False
        return abs(oi_delta_5m_pct) >= (thresholds.div_min_abs_oi or 0.0) and abs(mom_1m_pct) <= (thresholds.div_max_abs_mom or 0.0)
    else:
        if oi_delta_5m_pct is None:
            return True
        return sign(mom_1m_pct) == sign(oi_delta_5m_pct) or mom_1m_pct == 0.0

def ffill_step(self):
    """Advance one synthetic minute using last close; volume=0."""
    if self.prev_close is None:
        return
    close = self.prev_close
    # high=low=close; no volume -> stabilizes ATR/vol z-score without creating real bars
    self.update(close, close, close, 0.0)
