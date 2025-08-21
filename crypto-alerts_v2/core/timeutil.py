from datetime import datetime, time

def in_session(t: datetime, start: str, end: str) -> bool:
    # start/end format "HH:MM"
    h1, m1 = map(int, start.split(":"))
    h2, m2 = map(int, end.split(":"))
    ts, te = time(h1, m1), time(h2, m2)
    tt = t.time()
    return ts <= tt <= te
