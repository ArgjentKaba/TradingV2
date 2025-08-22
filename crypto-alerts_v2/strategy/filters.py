import pandas as pd

# -------------------------------------------------------------------
# SAFE / FAST Filter aus filters.yaml anwenden
# -------------------------------------------------------------------


def apply_filters(df: pd.DataFrame, filters_cfg: dict):
    """
    Wendet SAFE- und FAST-Filter auf das DataFrame an.
    Gibt eine Liste von Entry-Signalen zurück:
    [
        {"time": <utc_ms>, "price": <float>, "profile": "SAFE"},
        {"time": <utc_ms>, "price": <float>, "profile": "FAST"}
    ]
    """
    entries = []

    # SAFE-Profil
    safe_cfg = filters_cfg.get("safe", {})
    safe_entries = _scan_with_profile(df, safe_cfg, profile="SAFE")
    entries.extend(safe_entries)

    # FAST-Profil
    fast_cfg = filters_cfg.get("fast", {})
    fast_entries = _scan_with_profile(df, fast_cfg, profile="FAST")
    entries.extend(fast_entries)

    return entries


def _scan_with_profile(df: pd.DataFrame, cfg: dict, profile: str):
    """
    Hilfsfunktion: scannt das DataFrame mit den Filterparametern eines Profils.
    Hier nur Dummy-Logik → liefert bei jedem n-ten Candle einen Entry.
    """
    entries = []
    step = cfg.get("step", 500)  # Dummy: alle 500 Kerzen
    for i, row in enumerate(df.itertuples(index=False)):
        if i % step == 0:  # Platzhalter statt echter Berechnungen
            entries.append(
                {
                    "time": getattr(row, "time"),
                    "price": getattr(row, "close"),
                    "profile": profile,
                }
            )
    return entries
