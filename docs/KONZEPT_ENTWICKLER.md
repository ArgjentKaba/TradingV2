# 👨‍💻 Entwickler‑Konzept – v2 umgesetzt (Final)
## Überblick
**Status v2:** vollständig implementiert in crypto-alerts_v2_final.zip. Dieses Dokument fasst *umgesetzte* Spezifikationen zusammen und dient als Single Source für Entwickler.
## 3) Handelslogik v2 (umgesetzt)
### Exit B & Zeit‑Exit
- **Exit B:** SL 6 %, TP1 8 % (33 %) → Stop **BE**, TP2 12 % (67 %).
- **Zeit‑Exit 90 min** *(nur solange TP1/SL noch nicht gegriffen)*:
  - **unrealized ≥ +0.10 %** → Full‑Exit (TimeMax_90m_Profit).
  - **unrealized < 0 %** → BE‑Exit beim ersten Rücklauf (TimeMax_90m_BE).
  - **Kein Zeit‑Exit** wenn TP1 schon war (Kaskade läuft weiter: BE‑Stop, TP2).
- **Intrabar‑Priorität:**
  - **BEFORE_TP1:** SL vor TP1.
  - **AFTER_TP1:** Stop‑BE vor TP2.
### SAFE/FAST‑Filter‑Gate (ohne BTC‑Bestätigung)
- **Signale** nur bei erfüllten Schwellen:
  - Momentum 1 m (SAFE ≥ \|0.80 %\| · FAST ≥ \|0.30 %\|)
  - Vol‑zScore(20) (SAFE ≥ 2.0 · FAST ≥ 1.0)
  - ATR14 % (SAFE ≤ 1.8 % · FAST ≤ 2.5 %)
  - Divergenz OI/Preis (SAFE verboten; FAST erlaubt ab \|ΔOI\| ≥ 2 % & \|Momentum\| ≤ 0.30 %)
  - **ΔOI 5 m** (SAFE ≥ +3 %/≤ −3 % · FAST ≥ +1 %/≤ −1 %) **falls OI‑CSV vorhanden**; sonst wird ohne OI geprüft.
- **Governor:** Tageslimits SAFE 2–4 / FAST 5–10; **Cooldown pro Symbol** SAFE 30 min / FAST 10 min.
**Hinweis:** v2 ist **Long‑only**; Shorts kommen in v3.
## 6) Grenzen v2 (bewusst)
- **Keine Shorts**, **kein ML** (kommt in v3).
- **Keine Fees/Slippage** (kommt in v4 live).

## 7) Nächste Schritte (v3/v4 – kurz)
- **v3 ML:** Feature‑Engine, Labels inkl. Zeit‑Exits, Walk‑Forward+Kalibrierung, E[R]/p(SL)‑Gate, optional Liquidations‑Features.
- **v4 Live:** Exchange, OCO‑Orders, Fees/Slippage, Risk Controls, Shadow/Canary, Audit.