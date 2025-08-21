# 📘 Masterplan – v2 umgesetzt (Final)
## v1.0 – Basis Backtest (fertig)
- OHLCV‑Loader (1m CSV), PaperExec, Logger runs/trades.csv
- Session‑Filter: 07–21 UTC (EU/US)
## v2.0 – Strategie & Varianten (UMGESETZT)
**Status:** Implementiert in crypto-alerts_v2_final.zip.
**Lieferumfang v2:** - **Exit B**: SL 6 %, TP1 8 % (33 %) → Stop auf **Break‑Even**, TP2 12 % (67 %). - **Zeit‑Exit 90 min**: Falls bis T=90 min kein SL/TP1/TP2 → - **≥ +0.10 %**: Full‑Exit TimeMax_90m_Profit; - **< 0 %**: BE‑Exit beim ersten Rücklauf TimeMax_90m_BE. - Nicht aktiv, wenn TP1 bereits war. - **SAFE/FAST‑Filter (ohne BTC‑Bestätigung)**: Momentum 1 m, Volumen‑zScore(20), ATR14 %‑Cap, Divergenz‑Regel; ΔOI 5 m **optional** (wird genutzt, wenn OI‑CSV vorhanden ist). - **Governor inkl. Cooldown** pro Symbol: SAFE 30 min, FAST 10 min; Tageslimits SAFE 2–4 / FAST 5–10. - **4 Varianten (Start 10 k, SL 6 %)**: 1) risk 0.5 fast · 2) risk 1.0 fast · 3) risk 0.5 safe · 4) risk 1.0 safe - Sizing: Notional = (Equity × Risiko) / 0.06 → ~833 $ / ~1’667 $ je 10 k. - **Single Source of Truth (Configs)**: config/filters.yaml, config/thresholds.yaml, config/runtime.yaml (inkl. days_back = 30, universe_file = symbols.txt). - **CSV‑Schema (log_schema = v2)** erweitert um Zeit‑Exit‑Felder. - **Universe**: Es werden **nur** die Paare aus **symbols.txt** gehandelt.
**Outputs** - Je Variante eine CSV in runs/ + kombinierte runs/trades_all_variants.csv (inkl. Risk/Equity & Zeit‑Exit‑Infos).
## v3.0 – ML‑Entscheidungsmodul (als Nächstes)
- Short‑Support; Feature‑Engine (OI/Vol/Momentum/ATR/Divergenz/Liquidity/Session; optional Liquidations); Labels nach Exit‑B inkl. Zeit‑Exits; Walk‑Forward CV + Kalibrierung; Live‑Gate via E[R], p(SL).
## v4.0 – Live‑Handel (Ausblick)
- Exchange‑Anbindung, Market/Limit, OCO (TP/SL), Fees/Slippage, Robustheit, Risk Controls (Daily MaxDD), Shadow/Canary‑Rollout.
## v5.0 – Monitoring (Ausblick)
- Dashboard (Equity, Drawdown, Hit‑Rate, PnL), Alerts, Reports.