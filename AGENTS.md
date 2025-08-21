# AGENTS.md – Arbeitsregeln für Assistenten & Devs


## Projektziel
Trading-/Alerting-Tool (v2 fixpack) mit Backtests (SAFE/FAST), Exit B, 90‑Min Zeit-Exit, 4 Varianten; v3–v5 Roadmap (ML‑Gate, Live, Monitoring).


## Architektur- und Code-Regeln
- Python ≥ 3.10, Struktur: `src/`, `tests/`, `config/`, `runs/`.
- Keine Hardcoded-Pfade: Pfade/Parameter kommen aus `config/*.yaml`.
- Typisierung: möglichst strikt (mypy/pyright auf Kernmodulen).
- Logging: strukturierte CSV (Schema v2), RotatingFileHandler für Textlogs.
- Determinismus: fixe Seeds; neben CSV einen Config‑Snapshot ablegen.


## Stil & Qualität
- Formatter: **black**; Linter: **ruff**; Imports: **isort**.
- Tests: **pytest**, Mindestdeckung (Kernmodule) 80%.
- PRs klein & fokussiert; jede Änderung aktualisiert Doku/README.


## Commits (Conventional Commits)
- `feat:`, `fix:`, `refactor:`, `docs:`, `test:`, `chore:`, `perf:`, `ci:`
- Beispiel: `feat(filters): add SAFE/FAST 0.5/1.0 presets`


## Pull Requests – Checkliste
- [ ] Lint/Format ok (ruff/black)
- [ ] Tests ok (pytest)
- [ ] README/Docs aktualisiert
- [ ] CSV‑Schema unverändert oder dokumentierte Migration
- [ ] Keine Secrets (keine `.env` Inhalte, API‑Keys)


## Verbote
- Kein `except: pass`. Fehler entweder behandeln oder bewusst hochreichen.
- Keine Secrets/Keys im Repo. `.env` nur lokal.
- Kein Direkt‑Push auf `main` (außer Hotfix, mit Vermerk).


## Definition of Done (DoD)
- Lokal ausführbar, Lint/Tests grün, Doku aktualisiert.
- Reproduzierbare Ergebnisse (Seeds + Config‑Snapshot).