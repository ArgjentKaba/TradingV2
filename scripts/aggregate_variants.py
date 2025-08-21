#!/usr/bin/env python3
"""
Aggregate the last N days (default: 30) of backtest results per variant
and pick the best variant based on equity growth with a MaxDD cap.

Inputs (by default):
  runs/trades_SAFE_005bp.csv
  runs/trades_SAFE_010bp.csv
  runs/trades_FAST_005bp.csv
  runs/trades_FAST_010bp.csv
  (It will ignore runs/trades_all_variants.csv.)

Outputs (default to runs/):
  runs/summary_30d.csv
  runs/summary_30d.json
  runs/best_variant.txt

Usage:
  python scripts/aggregate_variants.py --runs-dir runs --out runs --days 30

Requires: pandas, numpy
"""
from __future__ import annotations
import argparse
import json
import os
import re
from typing import Dict, List

import numpy as np
import pandas as pd

TERMINAL_REASONS = (
    "ExitA_SL",              # full loss
    "ExitB_StopBE",         # 67% leg closed at BE (full flat)
    "ExitB_TP2",            # 67% leg closed at TP2 (full flat)
    "TimeMax_90m_Profit",   # time exit full
    "TimeMax_90m_BE",       # time exit at BE
)

VARIANT_NAME_MAP = {
    ("SAFE", 0.5): "risk 0.5 safe",
    ("SAFE", 1.0): "risk 1.0 safe",
    ("FAST", 0.5): "risk 0.5 fast",
    ("FAST", 1.0): "risk 1.0 fast",
}


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs-dir", default="runs", help="Directory containing trades_*.csv files")
    ap.add_argument("--out", default="runs", help="Where to write summary outputs")
    ap.add_argument("--days", type=int, default=30, help="How many trailing days to aggregate")
    return ap.parse_args()


def _list_variant_files(runs_dir: str) -> List[str]:
    files = []
    for fn in os.listdir(runs_dir):
        if not fn.endswith(".csv"):
            continue
        if fn == "trades_all_variants.csv":
            continue
        if not fn.startswith("trades_"):
            continue
        files.append(os.path.join(runs_dir, fn))
    return sorted(files)


def _normalize_variant_name(profile: str, risk_perc_run: str | float) -> str:
    # risk_perc_run is string like "0.50" or "1.00" (percent), or numeric (0.5 / 1.0)
    try:
        val = float(str(risk_perc_run))
        # If given as percent (e.g. "0.50"), keep as is; if given as 0.5, keep.
        # We want 0.5 or 1.0
        if val > 1.0:
            val = val / 100.0
    except Exception:
        val = 0.5
    profile = (profile or "SAFE").upper()
    return VARIANT_NAME_MAP.get((profile, round(val, 1)), f"{val:.1f}% {profile}")


def _is_terminal(reason: str) -> bool:
    return any(r in str(reason) for r in TERMINAL_REASONS)


def _coerce_float(s: object, default: float = np.nan) -> float:
    try:
        return float(str(s))
    except Exception:
        return default


def _max_drawdown_pct(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    roll_max = equity.cummax()
    dd = equity / roll_max - 1.0
    return float(dd.min() * 100.0)  # negative number (e.g., -4.2)


def _load_and_filter(path: str, days: int) -> pd.DataFrame:
    df = pd.read_csv(path)
    # Parse times
    for col in ("time_exit", "time_entry"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    # Filter to last N days by time_exit
    if "time_exit" in df.columns and df["time_exit"].notna().any():
        max_exit = df["time_exit"].max()
        cutoff = max_exit - pd.Timedelta(days=days)
        df = df[df["time_exit"] >= cutoff].copy()
    return df


def summarize_variant(df: pd.DataFrame, variant_label: str) -> Dict[str, float | int | str]:
    if df.empty:
        return {
            "variant": variant_label,
            "trades": 0,
            "trades_per_day": 0.0,
            "sum_pnl_usd": 0.0,
            "sum_pnl_pct": 0.0,
            "equity_start": 0.0,
            "equity_end": 0.0,
            "equity_change_pct": 0.0,
            "max_dd_pct": 0.0,
            "sl_rate": 0.0,
            "tp2_rate": 0.0,
            "avg_R_terminal": 0.0,
        }

    # derive terminal rows (one per full trade closure)
    terminal = df[df["reason"].astype(str).apply(_is_terminal)].copy()
    # trades per day
    if "time_exit" in terminal.columns and terminal["time_exit"].notna().any():
        days_span = max(1, (terminal["time_exit"].dt.normalize().nunique()))
    else:
        days_span = 30

    trades = int(len(terminal))
    trades_per_day = float(trades / days_span)

    # equity series (use equity_after when available)
    equity_series = df.sort_values("time_exit").get("equity_after")
    if equity_series is not None and equity_series.notna().any():
        equity = equity_series.astype(float)
        equity_start = float(df.sort_values("time_exit").get("equity_before").dropna().iloc[0]) if "equity_before" in df.columns and df["equity_before"].notna().any() else float(equity.iloc[0])
        equity_end = float(equity.iloc[-1])
        equity_change_pct = (equity_end / equity_start - 1.0) * 100.0 if equity_start else 0.0
        max_dd_pct = _max_drawdown_pct(equity)
    else:
        # fallback: use sum of USD PnL; assume start 10k
        start = 10000.0
        pnl_usd = df.get("account_pnl_usd", pd.Series([], dtype=float)).astype(float).sum()
        equity_start = start
        equity_end = start + float(pnl_usd)
        equity_change_pct = (equity_end / equity_start - 1.0) * 100.0
        max_dd_pct = 0.0

    # sums
    sum_pnl_usd = float(df.get("account_pnl_usd", pd.Series([], dtype=float)).astype(float).sum())
    # comp-based pct is reported as equity_change_pct; also provide raw sum of account_pnl_pct (not ideal for compounding)
    sum_pnl_pct = float(df.get("account_pnl_pct", pd.Series([], dtype=float)).astype(float).sum())

    # rates
    sl_count = int(terminal["reason"].astype(str).str.contains("ExitA_SL").sum())
    tp2_count = int(terminal["reason"].astype(str).str.contains("ExitB_TP2").sum())
    sl_rate = (sl_count / trades) if trades else 0.0
    tp2_rate = (tp2_count / trades) if trades else 0.0

    # avg R on terminal rows only (approximation)
    if "R_multiple" in terminal.columns:
        avg_R_terminal = float(pd.to_numeric(terminal["R_multiple"], errors="coerce").dropna().astype(float).mean() or 0.0)
    else:
        avg_R_terminal = 0.0

    return {
        "variant": variant_label,
        "trades": trades,
        "trades_per_day": round(trades_per_day, 3),
        "sum_pnl_usd": round(sum_pnl_usd, 2),
        "sum_pnl_pct": round(sum_pnl_pct, 3),
        "equity_start": round(equity_start, 2),
        "equity_end": round(equity_end, 2),
        "equity_change_pct": round(equity_change_pct, 3),
        "max_dd_pct": round(max_dd_pct, 3),
        "sl_rate": round(sl_rate, 3),
        "tp2_rate": round(tp2_rate, 3),
        "avg_R_terminal": round(avg_R_terminal, 3),
    }


def main():
    args = parse_args()
    os.makedirs(args.out, exist_ok=True)

    files = _list_variant_files(args.runs_dir)
    if not files:
        raise SystemExit(f"No trades_*.csv files found in {args.runs_dir}")

    summaries: List[Dict] = []

    for path in files:
        df = _load_and_filter(path, days=args.days)
        if df.empty:
            continue
        # Detect variant from columns (preferred) or filename
        prof = (df.get("profile_run") or df.get("profile") or pd.Series(["SAFE"]))
        risk = (df.get("risk_perc_run") or df.get("risk_perc") or pd.Series(["0.5"]))
        profile_val = str(prof.iloc[0]) if len(prof) > 0 else "SAFE"
        risk_val = str(risk.iloc[0]) if len(risk) > 0 else "0.5"
        variant_label = _normalize_variant_name(profile_val, risk_val)

        # Summarize
        summaries.append(summarize_variant(df, variant_label))

    if not summaries:
        raise SystemExit("No data after filtering.")

    # Rank: highest equity_change_pct with MaxDD cap 5%; ties by lowest sl_rate
    df_sum = pd.DataFrame(summaries)
    eligible = df_sum[df_sum["max_dd_pct"] >= -5.0]  # max_dd is negative; >= -5.0 means drawdown not worse than -5%
    if eligible.empty:
        eligible = df_sum.copy()
    ranked = eligible.sort_values(["equity_change_pct", "sl_rate"], ascending=[False, True]).reset_index(drop=True)

    # Write outputs
    out_csv = os.path.join(args.out, f"summary_{args.days}d.csv")
    out_json = os.path.join(args.out, f"summary_{args.days}d.json")
    ranked.to_csv(out_csv, index=False)
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(ranked.to_dict(orient="records"), f, indent=2)
    with open(os.path.join(args.out, "best_variant.txt"), "w", encoding="utf-8") as f:
        f.write(str(ranked.iloc[0]["variant"]))

    # Console print
    print("\nTop (MaxDD ≤ 5% preferred):\n", ranked.head(4).to_string(index=False))
    print(f"\nWrote: {out_csv}, {out_json}, best_variant.txt\n")


if __name__ == "__main__":
    main()
