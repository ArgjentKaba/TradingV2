from __future__ import annotations
import csv, os
from typing import List, Dict

ORDERED_FIELDS = [
    "time_entry","time_exit","symbol","side","entry","exit","pnl_pct","reason",
    "profile_run","risk_perc_run","R_multiple","account_pnl_pct","account_pnl_usd",
    "equity_before","equity_after","qty","notional_usd",
    "time_limit_applied","time_limit_minutes","unrealized_pct_at_90m","be_armed",
    "leg","leg_fraction"
]

KEY_ALIASES = {
    "profile": "profile_run",
    "risk_perc": "risk_perc_run",
}

def _normalize_row(row: Dict) -> Dict:
    r = dict(row)
    for old, new in KEY_ALIASES.items():
        if old in r and new not in r:
            r[new] = r[old]
    out = {}
    for k in ORDERED_FIELDS:
        out[k] = "" if r.get(k) is None else str(r.get(k))
    for k, v in r.items():
        if k not in out:
            out[k] = "" if v is None else str(v)
    return out

def write_trades(trades: List[Dict], path: str, use_risk_fields: bool = True):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not trades:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=ORDERED_FIELDS)
            writer.writeheader()
        return
    extra = []
    for t in trades:
        for k in t.keys():
            if k not in ORDERED_FIELDS and k not in extra:
                extra.append(k)
    header = ORDERED_FIELDS + extra
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for t in trades:
            writer.writerow(_normalize_row(t))
