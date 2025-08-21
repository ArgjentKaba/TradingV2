import pandas as pd, numpy as np

def zscore(s, n=20):
    mu = s.rolling(n, min_periods=max(5, n//2)).mean()
    sd = s.rolling(n, min_periods=max(5, n//2)).std(ddof=0)
    z = (s - mu) / sd
    return z.replace([np.inf,-np.inf], np.nan).fillna(0.0)

def winsorize(s, lo=0.01, hi=0.99):
    if len(s)==0: return s
    a, b = s.quantile(lo), s.quantile(hi)
    return s.clip(a,b)

def make_features(bars: pd.DataFrame, oi: pd.DataFrame|None, dir_: int, oi_available_series=None, winsor=(0.01,0.99)) -> pd.DataFrame:
    df = bars.copy()
    df["mom1"] = df["close"].pct_change(1).fillna(0.0)
    df["vol_z20"] = zscore(df["volume"].astype(float), 20)
    tr = (df["high"] - df["low"]).abs()
    df["atr_pct"] = (tr.rolling(14, min_periods=7).mean() / df["close"].replace(0, np.nan)).fillna(0.0)

    df["oi_available"] = 0
    if oi is not None and len(oi)>0 and oi_available_series is not None:
        oi_al = oi["oi"].reindex(df.index).interpolate(limit=2)
        df["oi_available"] = oi_available_series.reindex(df.index).fillna(0).astype(int)
        mask = df["oi_available"] == 1
        d5  = (oi_al - oi_al.shift(5)) / (oi_al.shift(5).replace(0, np.nan))
        d15 = (oi_al - oi_al.shift(15)) / (oi_al.shift(15).replace(0, np.nan))
        d15_z = zscore(d15, 20)
        def wz(s): 
            a,b = s.quantile(0.01), s.quantile(0.99); 
            return s.clip(a,b)
        d5 = wz(d5[mask]).reindex(df.index).fillna(0.0)
        d15 = wz(d15[mask]).reindex(df.index).fillna(0.0)
        d15_z = wz(d15_z[mask]).reindex(df.index).fillna(0.0)
        df["dOI_5"] = d5; df["dOI_15"] = d15; df["dOI_15_z"] = d15_z
    else:
        df["dOI_5"] = 0.0; df["dOI_15"] = 0.0; df["dOI_15_z"] = 0.0

    p5 = df["close"].pct_change(5).fillna(0.0)
    df["div_5"] = np.sign(p5) * np.sign(df["dOI_5"])
    df["prod_5"] = p5 * df["dOI_5"]

    idx = df.index.tz_convert("UTC")
    df["h_sin"] = np.sin(2*np.pi*idx.hour/24.0)
    df["h_cos"] = np.cos(2*np.pi*idx.hour/24.0)
    dow = pd.get_dummies(idx.dayofweek, prefix="dow"); dow.index = df.index
    df = pd.concat([df, dow], axis=1)

    s = 1 if dir_>=0 else -1
    df["mom1_dir"]   = s*df["mom1"]
    df["dOI_5_dir"]  = s*df["dOI_5"]
    df["dOI_15_dir"] = s*df["dOI_15"]
    df["prod_5_dir"] = s*df["prod_5"]

    feats = ["mom1_dir","vol_z20","atr_pct","dOI_5_dir","dOI_15_dir","dOI_15_z","div_5","prod_5_dir","h_sin","h_cos","oi_available"]
    feats += [c for c in df.columns if c.startswith("dow_")]
    return df[feats]
