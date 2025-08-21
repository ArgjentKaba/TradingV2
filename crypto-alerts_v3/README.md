# crypto-alerts (v2 fixpack)

This bundle contains the v2 backtester with:
- SAFE/FAST filter-gate (no BTC confirmation)
- Exit B (SL 6%, TP1 8%/33% -> BE, TP2 12%/67%)
- 90-minute time-exit (profit or BE)
- 4 variants (risk 0.5/1.0 × safe/fast)
- Single Source of Truth configs
- CSV schema v2 (incl. time-exit fields and leg markers)
- Variants loaded from `config/runtime.yaml`
- Gap handling per spec

## Run
```
python app.py
```

### Outputs (v2)
Nach `python app.py` entstehen:
- `runs/trades_SAFE_005bp.csv`
- `runs/trades_SAFE_010bp.csv`
- `runs/trades_FAST_005bp.csv`
- `runs/trades_FAST_010bp.csv`
- `runs/trades_all_variants.csv`

**CSV-Schema v2** enthält u. a.: `profile_run,risk_perc_run,R_multiple,account_pnl_*,
equity_*,qty,notional_usd,time_limit_applied,unrealized_pct_at_90m,be_armed,leg,leg_fraction`.


- Small gap handling: ≤ 2 min gaps are ffilled into indicators (no synthetic bars/entries).