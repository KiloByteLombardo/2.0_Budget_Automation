from __future__ import annotations
from typing import Dict
import pandas as pd

from core.Lectura import read_csv_resilient


def load_tipo_map_from_config(tp_cfg: Dict) -> pd.Series | None:
    """
    Lee el mini maestro PROVEEDOR->TIPO desde un Google Sheet publicado como CSV,
    según la configuración YAML (lookups.tipo_mercancia).

    Espera:
      tp_cfg = {
        "enabled": true,
        "source": "google_sheet_csv",
        "url": "https://.../export?format=csv&gid=XXXXX",
        "duplicate_policy": "first_row" | "last_row"   # opcional (default: last_row)
      }

    Devuelve:
      pandas.Series (index=PROVEEDOR, values=TIPO) con dedupe aplicado.
    """
    if not tp_cfg or not tp_cfg.get("enabled"):
        return None
    if (tp_cfg.get("source") or "").lower() != "google_sheet_csv":
        return None

    url = tp_cfg.get("url")
    if not url:
        return None

    df = read_csv_resilient(url)
    if df is None or df.empty:
        return None

    # Normaliza encabezados
    cols_up = {str(c).strip().upper(): c for c in df.columns}
    if "PROVEEDOR" not in cols_up or "TIPO" not in cols_up:
        return None

    c_prov = cols_up["PROVEEDOR"]
    c_tipo = cols_up["TIPO"]

    # Limpieza mínima
    df[c_prov] = (
        df[c_prov]
        .astype("string")
        .str.replace("\u00A0", " ", regex=False)  # NBSP -> espacio
        .str.strip()
    )
    df[c_tipo] = df[c_tipo].astype("string").str.strip()

    # Dedupe por proveedor
    policy = (tp_cfg or {}).get("duplicate_policy", "last_row")
    if policy == "first_row":
        df = df.drop_duplicates(subset=[c_prov], keep="first")
    else:
        df = df.drop_duplicates(subset=[c_prov], keep="last")

    # Construir Series (index literal)
    tipo_map = pd.Series(df[c_tipo].values, index=df[c_prov].values, dtype="string")
    return tipo_map


def apply_tipo_lookup(df: pd.DataFrame, tp_cfg: Dict, tipo_map: pd.Series | None) -> pd.DataFrame:
    """
    Escribe una columna (por defecto 'tipo_mercancia') a partir del mini maestro (tipo_map).
    Respeta match_policy si viene en el YAML.

    Por defecto:
      apply_to_sources = ["EBS","REIM","RSF"]
      on_column        = "proveedor"
      write_to         = "tipo_mercancia"
      overwrite_existing = True
      trace_field      = None
      trace_value      = "MAESTRO_TIPO_VE"
    """
    if tipo_map is None or getattr(tipo_map, "empty", True):
        return df

    df = df.copy()

    mp = (tp_cfg or {}).get("match_policy", {})
    apply_srcs  = set((mp.get("apply_to_sources") or ["EBS", "REIM", "RSF"]))
    on_col      = mp.get("on_column", "proveedor")
    out_col     = mp.get("write_to", "tipo_mercancia")
    overwrite   = bool(mp.get("overwrite_existing", True))
    trace_field = mp.get("trace_field")
    trace_value = mp.get("trace_value", "MAESTRO_TIPO_VE")

    # Determinar columna de fuente (APP u origen)
    app_col = "APP" if "APP" in df.columns else ("origen" if "origen" in df.columns else None)
    if app_col:
        mask_src = df[app_col].astype("string").str.upper().isin({s.upper() for s in apply_srcs})
    else:
        # Si no hay APP/origen, aplica a todas
        mask_src = pd.Series(True, index=df.index)

    # Columnas de trabajo
    if out_col not in df.columns:
        df[out_col] = pd.NA

    if overwrite:
        mask_need = mask_src
    else:
        cur = df[out_col]
        mask_need = mask_src & (cur.isna() | (cur.astype("string").str.len() == 0))

    if not mask_need.any():
        return df

    # Limpieza ligera del proveedor antes de mapear (NBSP + strip)
    prov = (
        df.loc[mask_need, on_col]
        .astype("string")
        .str.replace("\u00A0", " ", regex=False)
        .str.strip()
    )
    lk = prov.map(tipo_map)

    if overwrite:
        df.loc[mask_need & lk.notna(), out_col] = lk[lk.notna()].astype("string")
    else:
        blank = df.loc[mask_need, out_col].isna() | (df.loc[mask_need, out_col].astype("string").str.len() == 0)
        idx   = blank[blank].index
        df.loc[idx, out_col] = lk.loc[idx]

    if trace_field:
        df.loc[mask_need & lk.notna(), trace_field] = trace_value

    return df
