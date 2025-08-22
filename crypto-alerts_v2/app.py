from __future__ import annotations
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta, timezone
import os

import pandas as pd

from core.config_util import load_config
from core.timeutil import in_session
from exec.paper import PaperExec
from ilog.csvlog import write_trades
from strategy.governor import Governor
from strategy.filters import apply_filters  # <-- dein DataFrame-Scanner

# -------------------------------------------------
# Config laden
# -------------------------------------------------
CFG_FILTERS = load_config("config/filters.yaml", default={})
CFG_THRESH = load_config("config/thresholds.yaml", default={})
CFG_RUN = load_config("config/runtime.yaml", default={})

SESSION_START = str(CFG_RUN.get("session_start", "07:00"))
SESSION_END = str(CFG_RUN.get("session_end", "21:00"))
DAYS_BACK = int(CFG_RUN.get("days_back", 0) or 0)
FORCE_ACCEPT = bool(CFG_RUN.get("force_accept", False))
VARIANTS = CFG_RUN.get("variants") or ["SAFE:1.0", "FAST:1.0"]


# -------------------------------------------------
# Helper
# -------------------------------------------------
def normalize_symbol_for_csv(sym: str) -> str:
    # z.B. CYBER/USDT:USDT  -> CYBERUSDT
    if "/" in sym and ":" in sym:
        base = sym.split("/")[0]
        return f"{base}USDT"
    return sym


def load_symbols(fn: str) -> List[str]:
    out: List[str] = []
    with open(fn, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            out.append(s)
    return out


def _parse_time_cell(tval: str) -> Optional[datetime]:
    if not tval:
        return None
    # ISO
    try:
        dt = datetime.fromisoformat(tval)
        if dt.tzinfo is None:
            return dt
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    except Exception:
        pass
    # Unix(ms)
    try:
        ms = int(float(tval))
        return datetime.fromtimestamp(ms // 1000, tz=timezone.utc).replace(tzinfo=None)
    except Exception:
        return None


# -------------------------------------------------
# CSV -> DataFrame
# -------------------------------------------------
def load_df_for_symbol(data_path: str, symbol: str) -> pd.DataFrame:
    base = normalize_symbol_for_csv(symbol)
    cand = [
        os.path.join(data_path, f"{base}_1m.csv"),
        os.path.join(data_path, f"BINANCE_1m_{base}.csv"),
    ]
    fn = next((p for p in cand if os.path.exists(p)), None)
    if not fn:
        print(f"[LOAD] {symbol} ({base}) → keine Datei gefunden")
        return pd.DataFrame()

    # lese CSV robust
    df = pd.read_csv(fn)
    # mappe Spaltennamen
    colmap = {
        "time": "time",
        "t": "time",
        "open": "open",
        "o": "open",
        "high": "high",
        "h": "high",
        "low": "low",
        "l": "low",
        "close": "close",
        "c": "close",
        "volume": "volume",
        "v": "volume",
    }
    # vereinheitlichen
    df = df.rename(columns={k: v for k, v in colmap.items() if k in df.columns})
    req = ["time", "open", "high", "low", "close", "volume"]
    for r in req:
        if r not in df.columns:
            print(f"[WARN] {symbol} fehlt Spalte: {r}")
            return pd.DataFrame()

    # Zeit in UTC-naive datetime
    def _to_dt(x):
        if pd.isna(x):
            return pd.NaT
        # string/number → datetime
        if isinstance(x, (int, float)):
            try:
                return datetime.fromtimestamp(int(x) // 1000, tz=timezone.utc).replace(
                    tzinfo=None
                )
            except Exception:
                return pd.NaT
        try:
            dt = datetime.fromisoformat(str(x))
            if dt.tzinfo is None:
                return dt
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        except Exception:
            return pd.NaT

    df["time"] = df["time"].apply(_to_dt)
    df = df.dropna(subset=["time"]).reset_index(drop=True)

    # optional: auf DAYS_BACK beschneiden
    if DAYS_BACK > 0:
        cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(
            days=DAYS_BACK
        )
        df = df[df["time"] >= cutoff].reset_index(drop=True)

    # Sessionfilter hier schon anwenden (schneller)
    df = df[
        df["time"].apply(lambda t: in_session(t, SESSION_START, SESSION_END))
    ].reset_index(drop=True)

    print(
        f"[LOAD] {symbol} ({base}) → {len(df)} Zeilen geladen aus {os.path.basename(fn)}"
    )
    return df


# -------------------------------------------------
# Varianten parsen
# -------------------------------------------------
def _parse_variant(s: str) -> Optional[Dict[str, Any]]:
    try:
        prof, risk = s.split(":")
        return {"profile": prof.strip().upper(), "risk": float(risk)}
    except Exception:
        return None


def build_variants_list() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for v in VARIANTS:
        p = _parse_variant(v)
        if p:
            out.append(p)
    return out


# -------------------------------------------------
# Backtest: Entries → Trades (SL/TP1/TP2/TimeLimit)
# -------------------------------------------------
def backtest_from_entries(
    df: pd.DataFrame,
    symbol: str,
    entries: List[Dict[str, Any]],
    profile: str,
    risk_perc: float,
):
    EXIT = {
        "sl_pct": float(CFG_THRESH.get("sl_pct", 6.0)),
        "tp1_pct": float(CFG_THRESH.get("tp1_pct", 8.0)),
        "tp2_pct": float(CFG_THRESH.get("tp2_pct", 12.0)),
        "time_limit_min": int(CFG_THRESH.get("time_limit_min", 90)),
        "time_limit_profit_min_pct": float(
            CFG_THRESH.get("time_limit_profit_min_pct", 0.10)
        ),
    }

    # Governor aus filters.yaml (profiles.safe / profiles.fast)
    prof_cfg = (CFG_FILTERS.get("profiles") or CFG_FILTERS).get(profile.lower(), {})
    gov = Governor(
        profile=profile,
        trades_min_per_day=prof_cfg.get("governor_trades_per_day", {}).get("min", 2),
        trades_max_per_day=prof_cfg.get("governor_trades_per_day", {}).get("max", 4),
        cooldown_minutes=prof_cfg.get("cooldown_minutes", 30),
    )

    execu = PaperExec(symbol=symbol, profile=profile, risk_override=risk_perc)

    if df.empty or not entries:
        return execu.trades

    # Index für schnellen Zeit→Zeilen Index
    # Wir suchen per binary search (pandas get_indexer)
    times = pd.to_datetime(df["time"])
    # Sicherheit: als naive Datetimes (ohne tz)
    times = times.dt.tz_localize(None)

    for e in entries:
        t_val = e.get("time")
        price = float(e.get("price", 0.0))
        # Zeit normalisieren
        if isinstance(t_val, (int, float)):
            et = datetime.fromtimestamp(int(t_val) // 1000, tz=timezone.utc).replace(
                tzinfo=None
            )
        else:
            et = _parse_time_cell(str(t_val))
        if et is None:
            continue

        # Governor: darf heute noch?
        if not gov.can_trade(et, symbol) and not FORCE_ACCEPT:
            continue

        # Einstiegszeile finden: nächster Index, dessen time >= et
        # (nearest greater or equal)
        idx = times.searchsorted(pd.Timestamp(et), side="left")
        if idx >= len(df):
            continue

        row = df.iloc[idx]
        # Seite bestimmen: simple Heuristik – grüne Kerze → LONG, rote → SHORT
        # Falls open fehlt/gleich, fallback = LONG
        try:
            bar_open = float(row["open"])
            bar_close = float(row["close"])
        except Exception:
            bar_open = bar_close = price
        side = "LONG" if bar_close >= bar_open else "SHORT"

        entry_price = float(row["close"])
        entry_time = row["time"]

        # Exitlevels je nach Seite
        if side == "LONG":
            sl = entry_price * (1 - EXIT["sl_pct"] / 100.0)
            tp1 = entry_price * (1 + EXIT["tp1_pct"] / 100.0)
            tp2 = entry_price * (1 + EXIT["tp2_pct"] / 100.0)
        else:  # SHORT
            sl = entry_price * (1 + EXIT["sl_pct"] / 100.0)
            tp1 = entry_price * (1 - EXIT["tp1_pct"] / 100.0)
            tp2 = entry_price * (1 - EXIT["tp2_pct"] / 100.0)

        be_armed = False
        time_deadline = entry_time + timedelta(minutes=EXIT["time_limit_min"])
        exit_done = False

        # vorwärts iterieren bis Exit
        for j in range(idx, len(df)):
            r = df.iloc[j]
            high = float(r["high"])
            low = float(r["low"])
            close = float(r["close"])
            tnow = r["time"]

            # SL
            if side == "LONG":
                if low <= sl:
                    execu.execute_trade(
                        "LONG",
                        entry_price,
                        sl,
                        entry_time,
                        tnow,
                        "ExitA_SL",
                        time_limit_applied=False,
                    )
                    gov.register_exit(tnow, symbol)
                    exit_done = True
                    break
            else:
                if high >= sl:
                    execu.execute_trade(
                        "SHORT",
                        entry_price,
                        sl,
                        entry_time,
                        tnow,
                        "ExitA_SL",
                        time_limit_applied=False,
                    )
                    gov.register_exit(tnow, symbol)
                    exit_done = True
                    break

            # TP1 → BE arming
            if side == "LONG":
                if (not be_armed) and high >= tp1:
                    be_armed = True
                elif be_armed and low <= entry_price:
                    execu.execute_trade(
                        "LONG",
                        entry_price,
                        entry_price,
                        entry_time,
                        tnow,
                        "ExitB_BE",
                        time_limit_applied=False,
                    )
                    gov.register_exit(tnow, symbol)
                    exit_done = True
                    break
                elif high >= tp2:
                    execu.execute_trade(
                        "LONG",
                        entry_price,
                        tp2,
                        entry_time,
                        tnow,
                        "ExitC_TP2",
                        time_limit_applied=False,
                    )
                    gov.register_exit(tnow, symbol)
                    exit_done = True
                    break
            else:  # SHORT
                if (not be_armed) and low <= tp1:
                    be_armed = True
                elif be_armed and high >= entry_price:
                    execu.execute_trade(
                        "SHORT",
                        entry_price,
                        entry_price,
                        entry_time,
                        tnow,
                        "ExitB_BE",
                        time_limit_applied=False,
                    )
                    gov.register_exit(tnow, symbol)
                    exit_done = True
                    break
                elif low <= tp2:
                    execu.execute_trade(
                        "SHORT",
                        entry_price,
                        tp2,
                        entry_time,
                        tnow,
                        "ExitC_TP2",
                        time_limit_applied=False,
                    )
                    gov.register_exit(tnow, symbol)
                    exit_done = True
                    break

            # Time-Limit (vor TP1)
            if tnow >= time_deadline:
                if side == "LONG":
                    pnl_now = (close - entry_price) / entry_price * 100.0
                    if pnl_now >= EXIT["time_limit_profit_min_pct"]:
                        execu.execute_trade(
                            "LONG",
                            entry_price,
                            close,
                            entry_time,
                            tnow,
                            "ExitD_TimeMax_Profit",
                            time_limit_applied=True,
                        )
                    elif close <= entry_price:
                        execu.execute_trade(
                            "LONG",
                            entry_price,
                            entry_price,
                            entry_time,
                            tnow,
                            "ExitD_TimeMax_BE",
                            time_limit_applied=True,
                        )
                    else:
                        execu.execute_trade(
                            "LONG",
                            entry_price,
                            close,
                            entry_time,
                            tnow,
                            "ExitD_TimeMax_Close",
                            time_limit_applied=True,
                        )
                else:
                    pnl_now = (entry_price - close) / entry_price * 100.0
                    if pnl_now >= EXIT["time_limit_profit_min_pct"]:
                        execu.execute_trade(
                            "SHORT",
                            entry_price,
                            close,
                            entry_time,
                            tnow,
                            "ExitD_TimeMax_Profit",
                            time_limit_applied=True,
                        )
                    elif close >= entry_price:
                        execu.execute_trade(
                            "SHORT",
                            entry_price,
                            entry_price,
                            entry_time,
                            tnow,
                            "ExitD_TimeMax_BE",
                            time_limit_applied=True,
                        )
                    else:
                        execu.execute_trade(
                            "SHORT",
                            entry_price,
                            close,
                            entry_time,
                            tnow,
                            "ExitD_TimeMax_Close",
                            time_limit_applied=True,
                        )
                gov.register_exit(tnow, symbol)
                exit_done = True
                break

        if exit_done:
            gov.register_trade(entry_time)  # Exit zählt den Trade als „verbraucht“
        elif FORCE_ACCEPT:
            # Falls kein Exit gefunden (z. B. am Datei-Ende), forced close mit letztem Preis
            r = df.iloc[-1]
            last_close = float(r["close"])
            tnow = r["time"]
            execu.execute_trade(
                side,
                entry_price,
                last_close,
                entry_time,
                tnow,
                "ExitZ_ForcedClose",
                time_limit_applied=False,
            )
            gov.register_exit(tnow, symbol)
            gov.register_trade(entry_time)

    return execu.trades


# -------------------------------------------------
# main
# -------------------------------------------------
def main():
    os.makedirs("runs", exist_ok=True)
    symbols = load_symbols("symbols.txt")
    variants = build_variants_list()

    # Filter-Konfig an apply_filters weiterreichen:
    # In deinem filters.py erwartet apply_filters keys "safe", "fast".
    # Falls deine YAML "profiles: { safe:..., fast:... }" hat, ziehen wir das passend raus:
    filters_cfg = CFG_FILTERS.get("profiles") or CFG_FILTERS

    total_trades = 0
    for sym in symbols:
        df = load_df_for_symbol("data", sym)
        if df.empty:
            print(f"[WARN] no data for {sym}")
            continue

        # Entry-Signale einmal erzeugen
        all_entries = apply_filters(
            df.copy(), filters_cfg
        )  # erwartet 'time' & 'close' Spalten

        # Nach Profil gruppieren (SAFE/FAST)
        entries_by_profile: Dict[str, List[Dict[str, Any]]] = {"SAFE": [], "FAST": []}
        for e in all_entries:
            entries_by_profile.setdefault(e.get("profile", "SAFE"), []).append(e)

        # Pro Variante (Profil + Risiko) backtesten
        for v in variants:
            profile = v["profile"]
            risk = float(v["risk"])
            prof_entries = entries_by_profile.get(profile, [])
            trades = backtest_from_entries(df, sym, prof_entries, profile, risk)

            if trades:
                fn = os.path.join(
                    "runs",
                    f"trades_{normalize_symbol_for_csv(sym)}_{profile}_{risk}.csv",
                )
                write_trades(trades, fn)  # (trades, path)
                total_trades += len(trades)
                print(f"[OK] {sym} {v} → {len(trades)} trades")
            else:
                print(f"[NOOP] {sym} {v} → 0 trades")

    if total_trades > 0:
        print(f"[DONE] total {total_trades} trades → runs/")
    else:
        print(
            "[DONE] keine Trades generiert. Prüfe Filter-Step/Einstiegssignale oder setze force_accept=true zum Sanity-Check."
        )


if __name__ == "__main__":
    main()
