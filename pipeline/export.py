from __future__ import annotations
import pandas as pd
from typing import Dict
from core.utils import sanitize_sheet_name
from core.dtypes import to_dt
from .enrich import enrich_raw_sources

def apply_headers_and_order(df: pd.DataFrame, export_cfg: dict) -> pd.DataFrame:
    out = df.copy()
    headers_map = (export_cfg or {}).get("headers", {})
    order       = (export_cfg or {}).get("order")
    if headers_map: out = out.rename(columns=headers_map)
    if order:
        keep = [c for c in order if c in out.columns]
        out = out[keep]
    return out

def write_excel_with_raw(out_path: str,
                         consolidated_df: pd.DataFrame,
                         export_cfg: dict,
                         raw_sources: dict[str, pd.DataFrame] | None = None,
                         exec_mon: pd.Timestamp | None = None,
                         tipo_map: pd.Series | None = None):
    sheets = (export_cfg or {}).get("sheets", {}) or {}
    s_cons = sheets.get("consolidated", "Consolidado")
    s_ebs  = sheets.get("ebs_raw", "EBS (Original)")
    s_reim = sheets.get("reim_raw","REIM (Original)")
    s_rsf  = sheets.get("rsf_raw", "RSF (Original)")

    if (export_cfg or {}).get("filter_consolidated_by_en_alcance", False) and ("en_alcance" in consolidated_df.columns):
        consolidated_df = consolidated_df.loc[consolidated_df["en_alcance"] == True].copy()

    df_cons = apply_headers_and_order(consolidated_df, export_cfg)

    write_raw = bool((export_cfg or {}).get("write_sources_raw", False)) and (raw_sources is not None)
    enriched = enrich_raw_sources(raw_sources, exec_mon, tipo_map=tipo_map) if (write_raw and exec_mon is not None) else (raw_sources or {})

    used=set()
    def uniq(n: str)->str:
        base = sanitize_sheet_name(n); name=base; i=1
        while name.upper() in used:
            suf=f"_{i}"; name=sanitize_sheet_name(base[:31-len(suf)]+suf); i+=1
        used.add(name.upper()); return name

    s_cons = uniq(s_cons)
    if write_raw:
        s_ebs  = uniq(s_ebs)
        s_reim = uniq(s_reim)
        s_rsf  = uniq(s_rsf)

    with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
        df_cons.to_excel(xw, index=False, sheet_name=s_cons)
        if write_raw:
            if "EBS" in enriched and enriched["EBS"] is not None:
                enriched["EBS"].to_excel(xw, index=False, sheet_name=s_ebs)
            if "REIM" in enriched and enriched["REIM"] is not None:
                enriched["REIM"].to_excel(xw, index=False, sheet_name=s_reim)
            if "RSF" in enriched and enriched["RSF"] is not None:
                enriched["RSF"].to_excel(xw, index=False, sheet_name=s_rsf)
