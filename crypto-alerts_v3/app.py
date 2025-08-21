from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Dict
from datetime import datetime, timedelta
import csv, os, re

from core.config_util import load_config
from core.events import Bar
from core.timeutil import in_session
from exec.paper import PaperExec
from ilog.csvlog import write_trades
from strategy.governor import Governor
from strategy.filters import RollingStats, FilterThresholds, passes_gate, divergence_ok
from v3.infer.runtime_gate import Models as V3Models, load_thresholds as load_v3_thresholds, ml_gate_decision
from v3.features.build import make_features
import pandas as pd

CFG_FILTERS = load_config("config/filters.yaml", default={})
CFG_THRESH  = load_config("config/thresholds.yaml", default={})
CFG_RUN     = load_config("config/runtime.yaml", default={})

EXIT = CFG_THRESH.get("exit_b", {"sl_pct":6.0, "tp1_pct":8.0, "tp1_part":0.33, "tp2_pct":12.0})
TIME_EXIT = CFG_THRESH.get("time_exit", {"max_hold_minutes":90, "profit_buffer_pct":0.10})

def load_symbols(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        return [s.strip() for s in f if s.strip() and not s.startswith("#")]

def load_bars_for_symbol(data_path: str, symbol: str) -> List[Bar]:
    fn = os.path.join(data_path, f"{symbol}_1m.csv")
    if not os.path.exists(fn):
        return []
    rows = []
    with open(fn, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                t = row.get("time") or row.get("t")
                try:
                    t = datetime.fromisoformat(t)
                except Exception:
                    t = datetime.utcfromtimestamp(int(t)//1000)
                o = float(row.get("open", row.get("o", 0.0)))
                h = float(row.get("high", row.get("h", 0.0)))
                l = float(row.get("low", row.get("l", 0.0)))
                c = float(row.get("close", row.get("c", 0.0)))
                v = float(row.get("volume", row.get("v", 0.0)))
                rows.append(Bar(t=t, open=o, high=h, low=l, close=c, volume=v))
            except Exception:
                continue
    # dedupe: last wins
    ded = {}
    for b in rows:
        ded[b.t] = b
    rows = sorted(ded.values(), key=lambda x: x.t)
    return rows

def load_oi_map(data_path: str, symbol: str) -> Dict[datetime, float]:
    fn = os.path.join(data_path, "oi", f"{symbol}_oi_1m.csv")
    out = {}
    if not os.path.exists(fn):
        return out
    with open(fn, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                t = row.get("time") or row.get("t")
                try:
                    t = datetime.fromisoformat(t)
                except Exception:
                    t = datetime.utcfromtimestamp(int(t)//1000)
                oi = float(row.get("oi") or row.get("open_interest") or row.get("value"))
                out[t] = oi
            except Exception:
                continue
    return out

def oi_delta_5m_pct(oi_map: Dict[datetime,float], t: datetime) -> Optional[float]:
    if not oi_map:
        return None
    cur = oi_map.get(t)
    past = oi_map.get(t - timedelta(minutes=5))
    if cur is None or past is None or past == 0:
        return None
    return (cur / past - 1.0) * 100.0

def _parse_variant(s: str):
    s = s.strip().lower()
    prof = "SAFE" if "safe" in s else ("FAST" if "fast" in s else "SAFE")
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    risk = 0.5
    if m:
        risk = float(m.group(1))
    risk_frac = risk / 100.0 if risk > 1.0 else risk / 100.0  # 0.5 -> 0.005
    return (prof, risk_frac)

def _build_variants_from_cfg():
    vs = (CFG_RUN.get("variants") or [])
    if isinstance(vs, list) and vs:
        return [ _parse_variant(str(v)) for v in vs ]
    return [("SAFE",0.005),("SAFE",0.010),("FAST",0.005),("FAST",0.010)]

def thresholds_for(profile: str) -> FilterThresholds:
    prof = (CFG_FILTERS.get("profiles", {}).get(profile.lower(), {})) if isinstance(CFG_FILTERS, dict) else {}
    oi = prof.get("oi_delta_5m_pct", {})
    mom = prof.get("momentum_1m_pct", {})
    div = prof.get("divergence_oi_price", {})
    return FilterThresholds(
        oi_long_min = oi.get("long_min"),
        oi_short_max = oi.get("short_max"),
        vol_z_min = float(prof.get("volume_zscore_20_min", 0.0)),
        mom_long_min = float(mom.get("long_min", 0.0)),
        mom_short_max = float(mom.get("short_max", 0.0)),
        allow_divergence = bool(div.get("allowed", False)),
        div_min_abs_oi = div.get("min_abs_oi_pct"),
        div_max_abs_mom = div.get("max_abs_mom_pct"),
        atr14_pct_max = float(prof.get("atr14_pct_max", 100.0)),
        liq_percentile_min = float(prof.get("liquidity_percentile_30d_min", 0.0)),
        spread_bps_max = float(prof.get("spread_bps_max", 1e9)),
    )

VARIANTS = _build_variants_from_cfg()

def backtest_variant(bars: List[Bar], symbol: str, profile: str, risk_perc: float):
    # v3 ML-gate runtime setup
    v3_enabled = bool(CFG_RUN.get('use_ml_gate', True))
    v3_models = V3Models(base_dir='models') if v3_enabled else None
    v3_thr = load_v3_thresholds('config/thresholds.yaml') if v3_enabled else None
    bars_hist = []  # accumulate bars for feature build

    execu = PaperExec(symbol=symbol, profile=profile, risk_override=risk_perc,
                      sl_pct=EXIT["sl_pct"], tp1_pct=EXIT["tp1_pct"], tp2_pct=EXIT["tp2_pct"])
    prof_cfg = CFG_FILTERS.get("profiles", {}).get(profile.lower(), {})
    gov = Governor(profile=profile,
                   trades_min_per_day=prof_cfg.get("governor_trades_per_day", {}).get("min", 2),
                   trades_max_per_day=prof_cfg.get("governor_trades_per_day", {}).get("max", 4),
                   cooldown_minutes=prof_cfg.get("cooldown_minutes", 30))
    th = thresholds_for(profile)
    oi_map = load_oi_map("data", symbol)
    rs = RollingStats(vol_window=20)

    days_back = int(CFG_RUN.get("days_back", 0) or 0)
    cutoff = None
    if days_back > 0:
        cutoff = datetime.utcnow() - timedelta(days=days_back)

    in_pos = False
    entry_price = None
    entry_time = None
    pos_state = None
    be_armed = False
    time_limit_applied = False

    # gap control
    clean_streak = 0
    locked_after_big_gap = False

    for i in range(1, len(bars)):
        b_prev, b = bars[i-1], bars[i]
        if cutoff and b.t < cutoff:
            rs.update(b.high, b.low, b.close, b.volume)
            continue

# gap handling with ffill for small gaps
delta_min = (b.t - b_prev.t).total_seconds() / 60.0

# ≤ 2-minute gaps: forward-fill indicator state (no new bars, no entries)
if 1.0 < delta_min <= 2.0:
    missing = max(0, int(round(delta_min)) - 1)  # e.g., 2.0 -> 1 ffill step
    for _ in range(missing):
        rs.ffill_step()

# > 2-minute gaps: lock entries until 5 clean 1m bars
# gap handling

        delta_min = (b.t - b_prev.t).total_seconds() / 60.0
        if delta_min > 2.0:
            locked_after_big_gap = True
            clean_streak = 0
        elif abs(delta_min - 1.0) < 1e-6 and locked_after_big_gap:
            clean_streak += 1
        if locked_after_big_gap and clean_streak >= 5:
            locked_after_big_gap = False
            clean_streak = 0

        if not in_session(b.t, "07:00", "21:00"):
            rs.update(b.high, b.low, b.close, b.volume)
            continue

        rs.update(b.high, b.low, b.close, b.volume)
        mom = (b.close / b_prev.close - 1.0) * 100.0
        vol_z = rs.vol_zscore()
        atr_pct = rs.atr14_pct()
        oi5 = oi_delta_5m_pct(oi_map, b.t) if oi_map else None

        if not in_pos:
            if locked_after_big_gap:
                # no entries during gap recovery
                continue
            side = "LONG" if mom >= 0 else "SHORT"
            gate = passes_gate(side, abs(mom), vol_z, atr_pct, oi5, th) and divergence_ok(mom, oi5, th)
            # v3 ML-gate (optional)
            if gate and v3_enabled:
                # build bars dataframe up to current time
                # ensure time is UTC-aware
                if not bars_hist or bars_hist[-1]['t'] != b.t:
                    bars_hist.append({'t': b.t, 'open': b.open, 'high': b.high, 'low': b.low, 'close': b.close, 'volume': b.volume})
                df = pd.DataFrame(bars_hist)
                idx = pd.to_datetime(df['t'], utc=True)
                bars_df = df.set_index(idx)[['open','high','low','close','volume']]
                # OI optional: not used here (fallback)
                import numpy as np
                oi_av = pd.Series(0, index=bars_df.index, dtype=int)
                dir_sign = +1 if side == 'LONG' else -1
                feats = make_features(bars_df, oi=None, dir_=dir_sign, oi_available_series=oi_av)
                x = feats.iloc[-1].to_dict()
                ok_ml, info = ml_gate_decision(x, profile, v3_thr, v3_models)
                gate = gate and ok_ml
            if side == "SHORT" and not CFG_RUN.get('use_shorts', False):
                gate = False  # v2 long-only unless v3 shorts enabled
            if gate and gov.can_trade(b.t, symbol):
                in_pos = True
                entry_price = b.close
                entry_time = b.t
                pos_state = "ENTRY"
                be_armed = False
                time_limit_applied = False
                gov.register_trade(b.t)
                continue

        if in_pos and entry_price is not None and entry_time is not None:
            sl = entry_price * (1 - EXIT["sl_pct"]/100.0)
            tp1 = entry_price * (1 + EXIT["tp1_pct"]/100.0)
            tp2 = entry_price * (1 + EXIT["tp2_pct"]/100.0)

            # BEFORE_TP1: SL before TP1
            if pos_state == "ENTRY" and b.low <= sl:
                execu.execute_trade("LONG", entry_price, sl, entry_time, b.t, "ExitA_SL", time_limit_applied=False)
                gov.register_exit(b.t, symbol)
                in_pos = False; entry_price=entry_time=None; pos_state=None; be_armed=False
                continue

            if pos_state == "ENTRY" and b.high >= tp1:
                execu.execute_trade("LONG", entry_price, tp1, entry_time, b.t, "ExitB_TP1 (33%)", time_limit_applied=False)
                pos_state = "AFTER_TP1"

            # AFTER_TP1: BE before TP2
            if pos_state == "AFTER_TP1" and b.low <= entry_price:
                execu.execute_trade("LONG", entry_price, entry_price, entry_time, b.t, "ExitB_StopBE (67%)", time_limit_applied=False)
                gov.register_exit(b.t, symbol)
                in_pos = False; entry_price=entry_time=None; pos_state=None; be_armed=False
                continue

            if pos_state == "AFTER_TP1" and b.high >= tp2:
                execu.execute_trade("LONG", entry_price, tp2, entry_time, b.t, "ExitB_TP2 (67%)", time_limit_applied=False)
                gov.register_exit(b.t, symbol)
                in_pos = False; entry_price=entry_time=None; pos_state=None; be_armed=False
                continue

            # TIME EXIT (only if no TP1/SL yet)
            if pos_state == "ENTRY":
                max_hold = timedelta(minutes=int(TIME_EXIT.get("max_hold_minutes", 90)))
                profit_buffer = float(TIME_EXIT.get("profit_buffer_pct", 0.10))
                if b.t - entry_time >= max_hold and not time_limit_applied:
                    unrealized_pct = (b.close / entry_price - 1.0) * 100.0
                    if unrealized_pct >= profit_buffer:
                        execu.execute_trade("LONG", entry_price, b.close, entry_time, b.t, "TimeMax_90m_Profit",
                                            time_limit_applied=True, unrealized_pct_at_90m=unrealized_pct, be_armed=False)
                        gov.register_exit(b.t, symbol)
                        in_pos = False; entry_price=entry_time=None; pos_state=None; be_armed=False; time_limit_applied=True
                        continue
                    else:
                        be_armed = True
                        time_limit_applied = True

            if pos_state == "ENTRY" and be_armed and b.high >= entry_price:
                execu.execute_trade("LONG", entry_price, entry_price, entry_time, b.t, "TimeMax_90m_BE",
                                    time_limit_applied=True, unrealized_pct_at_90m=None, be_armed=True)
                gov.register_exit(b.t, symbol)
                in_pos = False; entry_price=entry_time=None; pos_state=None; be_armed=False
                continue

    for r in execu.trades:
        r["profile_run"] = profile
        r["risk_perc_run"] = f"{risk_perc*100:.2f}"
    return execu.trades

def main():
    symbols = load_symbols(CFG_RUN.get("universe_file", "symbols.txt"))
    combined = []
    out_dir = CFG_RUN.get("output", {}).get("dir", "runs")
    os.makedirs(out_dir, exist_ok=True)
    for profile, risk_perc in VARIANTS:
        all_trades = []
        for sym in symbols:
            bars = load_bars_for_symbol("data", sym)
            if not bars:
                print(f"⚠️ Keine Daten für {sym} gefunden.")
                continue
            trades = backtest_variant(bars, sym, profile=profile, risk_perc=risk_perc)
            all_trades.extend(trades)

        out_name = os.path.join(out_dir, f"trades_{profile}_{int(risk_perc*1000):03d}bp.csv")
        write_trades(all_trades, out_name, use_risk_fields=True)
        print(f"Fertig. {profile} @ {risk_perc*100:.2f}% Risiko → {len(all_trades)} Trades → {out_name}")
        combined.extend(all_trades)

    if CFG_RUN.get("output", {}).get("save_combined", True):
        write_trades(combined, os.path.join(out_dir, "trades_all_variants.csv"), use_risk_fields=True)
        print(f"Gesamt: {len(combined)} Trades → {os.path.join(out_dir, 'trades_all_variants.csv')}")

if __name__ == "__main__":
    main()
