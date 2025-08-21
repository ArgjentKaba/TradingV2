#!/usr/bin/env python3
"""
Fetch 90 days of 1m OHLCV for a given symbol list from Binance USDT-M Perpetual (Futures).
If a symbol is not available on Futures, fall back to Spot automatically.

Conforms to the V2 data hygiene spec:
- UTC timestamps, right-labelled on candle close
- Duplicate timestamps -> keep the last
- Gaps <= 2 minutes -> forward-fill (ffill) without creating new rows
- Gaps > 2 minutes -> leave as gap (no synthetic rows)
- Output CSV schema: time, open, high, low, close, volume
"""

import os
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
from tqdm import tqdm
import ccxt

# ----------------------------- Config -----------------------------
SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "CYBERUSDT",
    "CROSSUSDT",
    "PROVEUSDT",
    "SPKUSDT",
    "AVAAIUSDT",
    "PENGUUSDT",
    "CUSDT",
    "1000PEPEUSDT",
    "ZORAUSDT",
    "LDOUSDT",
    "MYXUSDT",
    "MYROUSDT",
    "1000BONKUSDT",
    "PORT3USDT",
    "PUMPUSDT",
    "FARTCOINUSDT",
    "WIFUSDT",
    "SKLUSDT",
    "AIOUSDT",
    "GTCUSDT",
]

TIMEFRAME = '1m'
DAYS_BACK = 90
DATA_DIR = os.path.join(os.getcwd(), 'data')
EXCHANGE_FUT = 'binanceusdm'
EXCHANGE_SPOT = 'binance'
MAX_RETRIES = 5
REQUEST_LIMIT = 1500
# -----------------------------------------------------------------

os.makedirs(DATA_DIR, exist_ok=True)

# --- Futures-Only Mapping ----------------------------------------
FUTURE_ONLY = {
    "CROSSUSDT": "CROSS/USDT:USDT",
    "AVAAIUSDT": "AVAAI/USDT:USDT",
    "1000PEPEUSDT": "1000PEPE/USDT:USDT",
    "ZORAUSDT": "ZORA/USDT:USDT",
    "MYXUSDT": "MYX/USDT:USDT",
    "MYROUSDT": "MYRO/USDT:USDT",
    "1000BONKUSDT": "1000BONK/USDT:USDT",
    "PORT3USDT": "PORT3/USDT:USDT",
    "PUMPUSDT": "PUMP/USDT:USDT",
    "FARTCOINUSDT": "FARTCOIN/USDT:USDT",
    "AIOUSDT": "AIO/USDT:USDT",   # Achtung: Coin heisst AIO
    "CUSDT": "C/USDT:USDT",       # Coin heisst nur "C"
}

def to_ccxt_symbol(sym: str) -> str:
    if sym in FUTURE_ONLY:
        return FUTURE_ONLY[sym]
    if sym.endswith('USDT'):
        base = sym[:-4]
        return f"{base}/USDT"
    if '/' in sym:
        return sym
    return sym

# --- Helpers -----------------------------------------------------
def utc_ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def ms_to_utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)

def new_exchange(exchange_id: str):
    cls = getattr(ccxt, exchange_id)
    return cls({
        'enableRateLimit': True,
        'options': {
            'defaultType': 'future' if exchange_id == EXCHANGE_FUT else 'spot',
        },
    })

# --- Data Fetch --------------------------------------------------
def fetch_all_ohlcv(ex, symbol: str, since_ms: int, timeframe: str) -> pd.DataFrame:
    all_rows = []
    next_since = since_ms
    last_len = -1

    for _ in range(200000):
        for attempt in range(MAX_RETRIES):
            try:
                chunk = ex.fetch_ohlcv(symbol, timeframe=timeframe, since=next_since, limit=REQUEST_LIMIT)
                break
            except ccxt.NetworkError:
                time.sleep(min(2 ** attempt, 30))
            except ccxt.BaseError as e:
                raise
        else:
            raise RuntimeError(f"Failed to fetch after {MAX_RETRIES} retries for {symbol}")

        if not chunk:
            break

        all_rows.extend(chunk)
        last_ts = chunk[-1][0]
        next_since = last_ts + ex.parse_timeframe(timeframe) * 1000

        if next_since >= utc_ms(now_utc() - timedelta(minutes=1)):
            break

        if last_len == len(all_rows):
            break
        last_len = len(all_rows)

    if not all_rows:
        return pd.DataFrame(columns=['time','open','high','low','close','volume'])

    df = pd.DataFrame(all_rows, columns=['time_ms','open','high','low','close','volume'])
    tf_sec = ex.parse_timeframe(timeframe)
    df['time'] = (df['time_ms'] + tf_sec * 1000 - 1).astype('int64')
    df.drop(columns=['time_ms'], inplace=True)
    df = df.drop_duplicates(subset=['time'], keep='last')
    df = df.sort_values('time').reset_index(drop=True)
    for c in ['open','high','low','close','volume']:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    df[['open','high','low','close','volume']] = df[['open','high','low','close','volume']].ffill()
    return df[['time','open','high','low','close','volume']]

def load_existing_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=['time','open','high','low','close','volume'])
    try:
        df = pd.read_csv(path)
        df = df.drop_duplicates(subset=['time'], keep='last')
        df = df.sort_values('time').reset_index(drop=True)
        return df[['time','open','high','low','close','volume']]
    except Exception:
        return pd.DataFrame(columns=['time','open','high','low','close','volume'])

def save_csv(path: str, df: pd.DataFrame):
    df.to_csv(path, index=False)

def symbol_available(ex, symbol: str) -> bool:
    markets = ex.load_markets()
    return symbol in markets

# --- Main --------------------------------------------------------
def main():
    start_since = now_utc() - timedelta(days=DAYS_BACK)
    since_ms = utc_ms(start_since)

    ex_fut = new_exchange(EXCHANGE_FUT)
    ex_spot = new_exchange(EXCHANGE_SPOT)

    for sym in tqdm(SYMBOLS, desc='Downloading'):
        ccxt_sym = to_ccxt_symbol(sym)
        out_path = os.path.join(DATA_DIR, f"BINANCE_1m_{sym}.csv")

        use_ex = ex_fut
        venue = 'FUTURES'
        try:
            if not symbol_available(ex_fut, ccxt_sym):
                if symbol_available(ex_spot, ccxt_sym):
                    use_ex = ex_spot
                    venue = 'SPOT'
                else:
                    print(f"[SKIP] {sym}: not found.")
                    continue
        except Exception:
            try:
                if symbol_available(ex_spot, ccxt_sym):
                    use_ex = ex_spot
                    venue = 'SPOT'
                else:
                    print(f"[SKIP] {sym}: unable to verify.")
                    continue
            except Exception:
                print(f"[SKIP] {sym}: market discovery failed.")
                continue

        existing = load_existing_csv(out_path)
        since_use = since_ms
        if not existing.empty:
            since_use = int(existing['time'].max()) + 1

        try:
            df = fetch_all_ohlcv(use_ex, ccxt_sym, since_use, TIMEFRAME)
        except ccxt.BaseError as e:
            print(f"[ERROR] {sym} ({venue}) fetch failed: {e}")
            continue

        if existing.empty:
            merged = df
        else:
            merged = pd.concat([existing, df], ignore_index=True)
            merged = merged.drop_duplicates(subset=['time'], keep='last')
            merged = merged.sort_values('time').reset_index(drop=True)

        if merged.empty:
            print(f"[WARN] {sym}: no data fetched.")
            continue

        save_csv(out_path, merged)
        time.sleep(0.2)

        first_dt = ms_to_utc(int(merged['time'].min()))
        last_dt  = ms_to_utc(int(merged['time'].max()))
        mins = len(merged)
        print(f"[OK] {sym} -> {venue} | rows={mins}, range={first_dt} .. {last_dt} | saved: {out_path}")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted by user.")
