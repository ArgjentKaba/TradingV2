# crypto-alerts (v2 fixpack)

Dieses Bundle enthält die **Version 2** des Backtesters mit:
- SAFE/FAST Filter-Gate (ohne BTC-Bestätigung)
- Exit B (SL 6%, TP1 8%/33% → BE, TP2 12%/67%)
- 90-Minuten Zeit-Exit (Profit oder Break-Even)
- 4 Varianten (Risk 0.5 / 1.0 × Safe / Fast)
- Single Source of Truth (Configs)
- CSV-Schema v2 (inkl. Zeit-Exit-Felder und Legs)
- Gap-Handling nach Spezifikation

---

## 🚀 Run
```bash
python app.py
```

### Outputs (v2)
Nach `python app.py` entstehen automatisch:
- `runs/trades_SAFE_005bp.csv`
- `runs/trades_SAFE_010bp.csv`
- `runs/trades_FAST_005bp.csv`
- `runs/trades_FAST_010bp.csv`
- `runs/trades_all_variants.csv`

**CSV-Schema v2** enthält u. a.:  
`profile_run, risk_perc_run, R_multiple, account_pnl_*, equity_*, qty, notional_usd, time_limit_applied, unrealized_pct_at_90m, be_armed, leg, leg_fraction`.

- Small gap handling: ≤ 2 min Gaps werden ffilled in Indikatoren (keine synthetischen Bars/Entries).

---

## 📖 Dokumentation & Konzepte

Die Details zu Strategie, Technik und Anwendung sind in drei Konzeptdokumenten abgelegt:

- [Masterplan (Strategie-Evolution, v1 → v5)](docs/MASTERPLAN.md)  
  > Überblick über Entwicklungsphasen und geplante Erweiterungen (ML, Live-Handel, Monitoring).  

- [Entwickler-Konzept (technische Umsetzung)](docs/KONZEPT_ENTWICKLER.md)  
  > Spezifikationen für Datenhygiene, Configs, Handelslogik, Risiko-Modelle und CSV-Schema.  

- [Trader-Konzept (Anwendung & Auswertung)](docs/KONZEPT_TRADER.md)  
  > Praktische Regeln, Varianten (Safe/Fast × Risiko), Datenbasis und Auswertung der Ergebnisse.  

---

## 🔮 Nächste Schritte
- v3: ML-Modul zur Signalbewertung (E[R], p(SL), Walk-Forward CV)  
- v4: Live-Handel mit Exchange-Anbindung, OCO-Orders, Fees/Slippage  
- v5: Monitoring (Dashboard, Alerts, Reports)
