from __future__ import annotations
import pandas as pd
from typing import Any, Dict
from core.dtypes import to_datetime_smart, apply_text_normalize, apply_value_maps, cast_dtypes, apply_filters

def normalize_source(df_raw: pd.DataFrame, src: str, cfg: Dict[str, Any], schema: Dict[str, Any]) -> pd.DataFrame:
    maps        = cfg["column_maps"][src]
    consts      = (cfg.get("const") or {})
    date_formats= cfg.get("date_formats", {})
    text_norm   = cfg.get("text_normalize", {})
    value_maps  = cfg.get("value_maps", {})
    filters     = (cfg.get("filters", {}) or {}).get(src, [])
    dtypes      = schema["dtypes"]

    rename_dict = {k: v for k, v in maps.items() if k in df_raw.columns}
    df = df_raw.rename(columns=rename_dict).copy()

    for k, v in consts.items(): df[k] = v
    df["origen"] = src.upper()

    if "fecha" in df.columns:
        fmt = date_formats.get(src)
        df["fecha"] = pd.to_datetime(df["fecha"], format=fmt, errors="coerce") if fmt else pd.to_datetime(df["fecha"], errors="coerce", dayfirst=True)

    for c in ("fecha_creacion","fecha_vencimiento","fecha_recepcion"):
        if c in df.columns:
            df[c] = to_datetime_smart(df[c])

    # tipado + normalizaciones
    df = apply_text_normalize(df, text_norm)
    df = apply_value_maps(df, value_maps)
    df = cast_dtypes(df, dtypes)
    df = apply_filters(df, filters)
    return df
