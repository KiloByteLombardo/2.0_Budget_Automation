from __future__ import annotations
import os
from datetime import datetime, timedelta, timezone
import pandas as pd
from typing import Any, Dict
from core.Lectura import read_csv_resilient

def load_priorities_from_config(pr_cfg: dict) -> pd.DataFrame | None:
    if not pr_cfg or not pr_cfg.get("enabled"): return None
    url = (pr_cfg or {}).get("url")
    if not url: return None

    cache = (pr_cfg or {}).get("cache", {})
    use_cache = cache.get("enabled", False)
    cache_path= cache.get("path")
    ttl_days  = cache.get("ttl_days", 1)

    if use_cache and cache_path and os.path.exists(cache_path):
        mtime = datetime.fromtimestamp(os.path.getmtime(cache_path), tz=timezone.utc)
        if (datetime.now(timezone.utc) - mtime) <= timedelta(days=ttl_days):
            try: return read_csv_resilient(cache_path)
            except Exception: pass

    df = read_csv_resilient(url)
    if use_cache and cache_path:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        df.to_csv(cache_path, index=False)
    return df

def _prov_key_nospaces(series: pd.Series) -> pd.Series:
    s = series.astype("string").str.replace("\u00A0"," ", regex=False)
    return s.str.replace(r"\s+","", regex=True)

def apply_priority_lookup(df: pd.DataFrame, pr_cfg: dict, master: pd.DataFrame) -> pd.DataFrame:
    if master is None or master.empty: return df
    if "PROVEEDOR" not in master.columns or "PRIORIDAD" not in master.columns:
        raise ValueError("El maestro de prioridades debe tener columnas PROVEEDOR y PRIORIDAD.")

    df = df.copy()
    mp = (pr_cfg or {}).get("match_policy", {})
    apply_srcs = set(mp.get("apply_to_sources", ["REIM","RSF"]))
    on_col     = mp.get("on_column", "proveedor")
    out_col    = mp.get("write_to", "prioridad")
    overwrite  = bool(mp.get("overwrite_existing", False))
    trace_f    = mp.get("trace_field")
    trace_val  = (pr_cfg or {}).get("trace_value", "MAESTRO_SHEET")
    default_pr = mp.get("default_priority")

    app_col = "APP" if "APP" in df.columns else ("origen" if "origen" in df.columns else None)
    mask_src = df[app_col].astype("string").str.upper().isin({s.upper() for s in apply_srcs}) if app_col else False
    cur = df.get(out_col); 
    if cur is None: df[out_col] = pd.NA; cur = df[out_col]
    need = mask_src & (overwrite | (cur.isna() | (cur.astype("string").str.len()==0)))
    if not need.any(): return df

    m = master.copy()
    m["__PROV_KEY_NS"] = _prov_key_nospaces(m["PROVEEDOR"])
    m = m.drop_duplicates(["__PROV_KEY_NS"], keep="first")

    left = df.loc[need, [on_col]].copy()
    left["__PROV_KEY_NS"] = _prov_key_nospaces(left[on_col])

    joined = left[["__PROV_KEY_NS"]].merge(m[["__PROV_KEY_NS","PRIORIDAD"]], on="__PROV_KEY_NS", how="left")

    df.loc[need, out_col] = df.loc[need, out_col].astype("string").where(
        df.loc[need, out_col].astype("string").str.len()>0, joined["PRIORIDAD"].values
    )

    if trace_f:
        has = joined["PRIORIDAD"].notna()
        df.loc[need & has, trace_f] = trace_val
        if default_pr is not None:
            no = ~has
            df.loc[need & no, out_col] = df.loc[need & no, out_col].where(
                df.loc[need & no, out_col].astype("string").str.len()>0, str(default_pr)
            )
            df.loc[need & no, trace_f] = df.loc[need & no, trace_f].fillna("DEFAULT")

    return df
