import yaml, numpy as np
from pathlib import Path
from joblib import load

def load_thresholds(path='config/thresholds.yaml'):
    cfg = yaml.safe_load(Path(path).read_text(encoding='utf-8'))
    # support both 'ml_gate' and legacy 'ml_thresholds'
    thr = cfg.get('ml_gate') or cfg.get('ml_thresholds')
    # normalize key casing
    norm = {}
    for k,v in thr.items():
        key = k.lower()
        if 'psl_max' in v or 'ER_min' in v or 'R_min' in v or 'pSL_max' in v:
            # map fields to psl_max, ER_min
            psl_max = v.get('psl_max', v.get('pSL_max'))
            er_min  = v.get('ER_min', v.get('R_min'))
            norm[key] = {'psl_max': float(psl_max), 'ER_min': float(er_min)}
    return norm

class Models:
    def __init__(self, base_dir='models'):
        self.psl = load(Path(base_dir)/'v3_psl_model.joblib')
        self.iso = load(Path(base_dir)/'v3_psl_iso.joblib')
        self.er  = load(Path(base_dir)/'v3_er_model.joblib')

def ml_gate_decision(features_row: dict, profile: str, thresholds: dict, models: Models):
    profile = profile.lower()
    thr = thresholds[profile]
    # align features
    if hasattr(models.psl, 'feature_names_in_'):
        cols = list(models.psl.feature_names_in_)
    else:
        cols = sorted(features_row.keys())
    import numpy as np
    X = np.array([[features_row.get(c, 0.0) for c in cols]], dtype=float)
    if hasattr(models.psl, 'predict_proba'):
        psl_raw = models.psl.predict_proba(X)[:,1]
    else:
        psl_raw = models.psl.decision_function(X)
    psl = float(models.iso.transform(psl_raw)[0])
    er  = float(models.er.predict(X)[0])
    ok = (psl <= thr['psl_max']) and (er >= thr['ER_min'])
    return ok, {'psl': psl, 'ER': er, 'profile': profile, 'thresh': thr}
