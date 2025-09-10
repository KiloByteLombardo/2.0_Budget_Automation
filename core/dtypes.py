from __future__ import annotations
import pandas as pd
from typing import Any, Dict, List
from .utils import ISO_PATTERN

def _strip_weird(s: pd.Series) -> pd.Series:
    return (s.astype("string")
             .str.replace("\u00A0", " ", regex=False)
             .str.replace("[\u200B\u200C\u200D\uFEFF]", "", regex=True)
             .str.strip())

def to_datetime_smart(series: pd.Series) -> pd.Series:
    """
    Si es ISO (YYYY-MM-DD [HH:MM:SS]) parsea con dayfirst=False;
    resto con dayfirst=True; y fallback a serial Excel.
    """
    s = _strip_weird(series)
    is_iso = s.str.match(ISO_PATTERN, na=False)

    out_iso  = pd.to_datetime(s.where(is_iso),   errors="coerce", dayfirst=False)
    out_rest = pd.to_datetime(s.where(~is_iso),  errors="coerce", dayfirst=True)
    out = out_iso.fillna(out_rest)

    need = out.isna() & s.notna()
    if need.any():
        num = pd.to_numeric(s, errors="coerce")
        out = out.mask(need, pd.to_datetime(num, errors="coerce", unit="D", origin="1899-12-30"))
    return out

def smart_to_numeric(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    need = s.isna() & series.notna()
    if need.any():
        s_alt = (
            series.astype("string")
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        s2 = pd.to_numeric(s_alt, errors="coerce")
        s = s.mask(need, s2)
    return s

def apply_text_normalize(df: pd.DataFrame, norm_cfg: Dict[str, List[str]]) -> pd.DataFrame:
    strip_cols = (norm_cfg or {}).get("strip", [])
    upper_cols = (norm_cfg or {}).get("upper", [])
    lower_cols = (norm_cfg or {}).get("lower", [])
    for c in strip_cols:
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip()
    for c in upper_cols:
        if c in df.columns:
            df[c] = df[c].astype("string").str.upper()
    for c in lower_cols:
        if c in df.columns:
            df[c] = df[c].astype("string").str.lower()
    return df

def apply_value_maps(df: pd.DataFrame, maps: Dict[str, Dict[str, str]]) -> pd.DataFrame:
    for col, mapping in (maps or {}).items():
        if col in df.columns:
            df[col] = df[col].replace(mapping)
    return df

def to_datetime_robust(series: pd.Series) -> pd.Series:
    """Convierte a datetime manejando NBSP/espacios y serial Excel como fallback."""
    s = series.astype("string")
    # limpia caracteres invisibles y espacios alrededor
    s = s.str.replace("\u00A0", " ", regex=False).str.strip()

    # 1) intento directo (soporta yyyy-mm-dd, dd/mm/aa, dd-mes-aa, etc.)
    dt = pd.to_datetime(s, errors="coerce", dayfirst=True)

    # 2) fallback: números tipo serial de Excel
    need = dt.isna() & s.notna()
    if need.any():
        num = pd.to_numeric(s, errors="coerce")
        dt2 = pd.to_datetime(num, errors="coerce", unit="D", origin="1899-12-30")
        dt = dt.mask(need, dt2)

    return dt


def cast_dtypes(df: pd.DataFrame, dtypes: Dict[str, str]) -> pd.DataFrame:
    for col, dtype in dtypes.items():
        if col not in df.columns:
            df[col] = pd.NA
        try:
            if dtype.startswith("datetime64"):
                df[col] = to_datetime_smart(df[col])
            elif dtype == "string":
                df[col] = df[col].astype("string")
            else:
                if "float" in dtype or "int" in dtype:
                    df[col] = smart_to_numeric(df[col])
                else:
                    df[col] = df[col].astype(dtype, errors="ignore")
        except Exception:
            pass
    return df

def apply_filters(df: pd.DataFrame, expressions: List[str]) -> pd.DataFrame:
    for expr in expressions or []:
        try:
            df = df.query(expr)
        except Exception:
            try:
                mask = eval(f"df.{expr}")
                df = df[mask]
            except Exception:
                raise
    return df

def to_dt(s: pd.Series) -> pd.Series:
    if s is None:
        return pd.Series(pd.NaT, index=[])
    # primer intento: dayfirst (dd/mm/aa, dd-mes-aa)
    dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
    # segundo intento: ISO (yyyy-mm-dd HH:MM:SS)
    need = dt.isna() & s.notna()
    if need.any():
        dt2 = pd.to_datetime(s[need], errors="coerce", dayfirst=False)
        dt = dt.mask(need, dt2)
    # tercer intento: serial de Excel
    need = dt.isna() & s.notna()
    if need.any():
        num = pd.to_numeric(s[need], errors="coerce")
        dt3 = pd.to_datetime(num, errors="coerce", unit="D", origin="1899-12-30")
        dt = dt.mask(need, dt3)
    return dt
