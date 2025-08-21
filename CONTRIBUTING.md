# CONTRIBUTING

## Setup
```bash
python -m venv .venv
# Linux/macOS
source .venv/bin/activate
# Windows
# .venv\Scripts\activate
pip install -r requirements.txt
```

## Dev-Kommandos
```bash
ruff check . && ruff format .
pytest -q
```

## Branch-Strategie
- `main`: stabil, versioniert
- `dev`: Integration
- Feature-Branches: `feature/<kurzbeschreibung>`

## Commit-Regeln (Conventional Commits)
- Präfixe: `feat:`, `fix:`, `refactor:`, `docs:`, `test:`, `chore:`, `perf:`, `ci:`
- Betreff < 72 Zeichen; Body erklärt **Warum/Wie** der Änderung

## PR-Flow
1. Branch erstellen
2. Lokal Lint/Format/Tests ausführen
3. PR gegen `dev` (oder direkt `main`, wenn klein/ungefährlich)
4. PR-Template ausfüllen, Checkliste abhaken

## Qualität & Tests
- Lint/Format: **ruff/black** (keine Lint-Warnungen im CI)
- Tests: **pytest** (wichtige Module abdecken)
- Reproduzierbarkeit: fixe Seeds; Config-Snapshot neben CSV-Outputs

## Sicherheit
- Keine Secrets/Keys im Repo (nutze `.env` lokal)
- Keine direkten Pushes auf `main` ohne Review/Hotfix-Vermerk