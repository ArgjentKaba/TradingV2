import json

def _load_text(path: str):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception:
        return None

def _try_json(txt: str):
    try:
        return json.loads(txt)
    except Exception:
        return None

def _try_yaml(txt: str):
    try:
        import yaml  # type: ignore
        return yaml.safe_load(txt)
    except Exception:
        return None

def load_config(path: str, default: dict) -> dict:
    txt = _load_text(path)
    if not txt:
        return default
    data = _try_json(txt)
    if data is None:
        data = _try_yaml(txt)
    if data is None:
        return default
    out = dict(default)
    out.update(data)
    return out
