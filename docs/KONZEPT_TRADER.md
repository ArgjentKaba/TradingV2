# 📊 Trader‑Konzept – v2 umgesetzt (Final)
## Was du mit v2 bekommst (heute)
- **app.py**** ****starten** → Strategie wird auf deinen Daten simuliert.
- **Regeln pro Trade:**
  - **Exit B**: SL 6 % · TP1 8 % (33 %) → Stop **Break‑Even** · TP2 12 % (67 %).
  - **Zeit‑Exit 90 min** (nur wenn bis dahin kein SL/TP1/TP2):
    - **im Plus (≥ +0.10 %)** → sofort schließen (TimeMax_90m_Profit).
    - **im Minus (< 0 %)** → auf **Break‑Even** warten und schließen (TimeMax_90m_BE).
    - **nicht** anwenden, wenn TP1 schon war.
## SAFE vs. FAST (Filter aktiv)
- **SAFE** = strenge Filter → **2–4 Trades/Tag** (hoher OI‑/Vol‑Impuls, klares Momentum, keine Divergenz, moderate Vola/enger Spread).
- **FAST** = lockere Filter → **5–10 Trades/Tag** (kleinere Impulse ok, Divergenz erlaubt, mehr Vola/Spread toleriert).
- **Hinweis:** OI wird genutzt, **wenn OI‑Dateien vorhanden** sind; ansonsten laufen die Filter ohne OI weiter.
## Unsere 4 Varianten (laufen automatisch)
- **risk 0.5 fast** (~833 $ je 10 k Startkapital)
- **risk 1.0 fast** (~1’667 $ je 10 k)
- **risk 0.5 safe** (~833 $ je 10 k)
- **risk 1.0 safe** (~1’667 $ je 10 k)
## Daten & Universe
- Es werden **nur** die Paare aus **symbols.txt** gehandelt.
- Standard‑Zeitraum für Läufe: **30 Tage** (konfigurierbar).
## Ausgaben (zum Vergleichen)
- Pro Variante eine CSV in runs/ **+** eine kombinierte trades_all_variants.csv.
- Enthaltene Kennzahlen u. a.: **PnL %/USD**, **Equity vor/nach**, **R‑Multiple**, **Gründe für Exits** (TP1/TP2/SL/Zeit‑Exit).
## Nächste Ausbaustufen (kurz)
- **v3:** ML bewertet die Qualität der Signale (E[R], p(SL)) und entscheidet strenger/lockerer je Profil.
- **v4:** Live‑Handel mit denselben Regeln, realen Gebühren/Slippage und vollständig protokollierten Orders.