from __future__ import annotations
import pandas as pd
from pathlib import Path
from typing import Any, Dict
from core.Lectura import load_yaml, read_source
from pipeline.normalize import normalize_source
from pipeline.post import apply_post
from core.dtypes import cast_dtypes
from lookups.prioridad import load_priorities_from_config, apply_priority_lookup
from lookups.factoring import load_factoring_from_config, apply_factoring_lookup
from lookups.tipo import load_tipo_map_from_config, apply_tipo_lookup

def run_colombia_mercancia(schema_path: str, country_path: str,
                           ebs_path: str, reim_path: str, rsf_path: str,
                           exec_date: pd.Timestamp | None = None) -> pd.DataFrame:
    schema = load_yaml(schema_path)["mercancia"]
    cfg    = load_yaml(country_path)["mercancia"]
    dtypes = schema["dtypes"]; inputs = cfg.get("inputs", {})

    ebs_df  = read_source(Path(ebs_path),  inputs.get("ebs", {}))
    reim_df = read_source(Path(reim_path), inputs.get("reim", {}))
    rsf_df  = read_source(Path(rsf_path),  inputs.get("rsf", {}))

    ebs_n  = normalize_source(ebs_df,  "ebs",  cfg, schema)
    reim_n = normalize_source(reim_df, "reim", cfg, schema)
    rsf_n  = normalize_source(rsf_df,  "rsf",  cfg, schema)

    ebs_n["APP"]="EBS"; reim_n["APP"]="REIM"; rsf_n["APP"]="RSF"
    base = pd.concat([ebs_n, reim_n, rsf_n], ignore_index=True, sort=False)

    # Patch robusto fecha_creacion en EBS
    mask_ebs = base.get("APP", pd.Series("", index=base.index)).eq("EBS")
    if "fecha_creacion" in base.columns:
        fc = pd.to_datetime(base.loc[mask_ebs,"fecha_creacion"], errors="coerce", dayfirst=True)
    else:
        fc = pd.Series(pd.NaT, index=base.index)
    if "fecha" in base.columns:
        alt = pd.to_datetime(base.loc[mask_ebs,"fecha"], errors="coerce", dayfirst=True)
        fc = fc.combine_first(alt)
    base.loc[mask_ebs, "fecha_creacion"] = fc

    # Lookups
    pr_cfg = (cfg.get("lookups", {}) or {}).get("prioridades", {})
    if pr_cfg.get("enabled"): 
        master = load_priorities_from_config(pr_cfg)
        if master is not None and not master.empty:
            base = apply_priority_lookup(base, pr_cfg, master)

    fx_cfg = (cfg.get("lookups", {}) or {}).get("factoring", {})
    if fx_cfg.get("enabled"):
        master_fx = load_factoring_from_config(fx_cfg)
        if master_fx is not None and not master_fx.empty:
            base = apply_factoring_lookup(base, fx_cfg, master_fx)

    # exec_mon (lunes)
    if exec_date is None: exec_date = pd.Timestamp.today().normalize()
    exec_mon = exec_date - pd.to_timedelta(exec_date.weekday(), unit="D")
    base = apply_post(base, cfg.get("post", {}), context={"exec_mon": exec_mon})
    base = cast_dtypes(base, dtypes)

    order = schema.get("order", [])
    final_cols = [c for c in order if c in base.columns]
    out = base[final_cols] if final_cols else base

    export_cfg = cfg.get("export", {})
    headers_map = export_cfg.get("headers", {})
    export_order = export_cfg.get("order")
    if headers_map:
        out = out.rename(columns=headers_map)
        if export_order:
            keep = [c for c in export_order if c in out.columns]
            out = out[keep]
    return out

def run_venezuela_mercancia(schema_path: str, country_path: str,
                            ebs_path: str, reim_path: str, rsf_path: str,
                            exec_date: pd.Timestamp | None = None) -> tuple[pd.DataFrame, dict, dict]:
    schema = load_yaml(schema_path)["mercancia"]
    cfg    = load_yaml(country_path)["mercancia"]
    dtypes = schema["dtypes"]; inputs = cfg.get("inputs", {})

    # Crudos para tabs
    ebs_df  = read_source(Path(ebs_path),  inputs.get("ebs", {}))
    reim_df = read_source(Path(reim_path), inputs.get("reim", {}))
    rsf_df  = read_source(Path(rsf_path),  inputs.get("rsf", {}))
    raw_sources = {"EBS": ebs_df, "REIM": reim_df, "RSF": rsf_df}

    # Normalizados
    ebs_n  = normalize_source(ebs_df,  "ebs",  cfg, schema); ebs_n["APP"]="EBS"
    reim_n = normalize_source(reim_df, "reim", cfg, schema); reim_n["APP"]="REIM"
    rsf_n  = normalize_source(rsf_df,  "rsf",  cfg, schema); rsf_n["APP"]="RSF"

    base = pd.concat([ebs_n, reim_n, rsf_n], ignore_index=True, sort=False)

    # Mini maestro TIPO
    tp_cfg = (cfg.get("lookups", {}) or {}).get("tipo_mercancia", {})
    tipo_map = None
    if tp_cfg.get("enabled"):
        tipo_map = load_tipo_map_from_config(tp_cfg)
        mpc = (tp_cfg or {}).get("match_policy_consolidated", {})
        if mpc and mpc.get("enabled") and (tipo_map is not None and not tipo_map.empty):
            apply_srcs = set(mpc.get("apply_to_sources", ["EBS","REIM","RSF"]))
            on_col     = mpc.get("on_column", "proveedor")
            out_col    = mpc.get("write_to", "tipo")
            overwrite  = bool(mpc.get("overwrite_existing", False))
            trace_f    = mpc.get("trace_field")
            trace_val  = mpc.get("trace_value", "MAESTRO_TIPO")

            app_col = "APP" if "APP" in base.columns else ("origen" if "origen" in base.columns else None)
            mask_src = base[app_col].astype("string").str.upper().isin({s.upper() for s in apply_srcs}) if app_col else False
            if out_col not in base.columns: base[out_col] = pd.NA

            need = mask_src & (overwrite | (base[out_col].isna() | (base[out_col].astype("string").str.len()==0)))
            if need.any():
                prov = base.loc[need, on_col].astype("string").str.replace("\u00A0"," ", regex=False).str.strip()
                lk = prov.map(tipo_map)
                base.loc[need & lk.notna(), out_col] = lk[lk.notna()]
                if trace_f:
                    base.loc[need & lk.notna(), trace_f] = trace_val

    # exec_mon
    if exec_date is None: exec_date = pd.Timestamp.today().normalize()
    exec_mon = exec_date - pd.to_timedelta(exec_date.weekday(), unit="D")
    base = apply_post(base, cfg.get("post", {}), context={"exec_mon": exec_mon})
    base = cast_dtypes(base, dtypes)

    order = schema.get("order", [])
    final_cols = [c for c in order if c in base.columns]
    out = base[final_cols] if final_cols else base

    export_cfg = (cfg.get("export", {}) or {})
    export_cfg["__tipo_map"] = tipo_map  # para GUI/export
    return out, {"EBS": ebs_df, "REIM": reim_df, "RSF": rsf_df}, export_cfg
