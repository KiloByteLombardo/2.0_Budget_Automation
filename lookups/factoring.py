from __future__ import annotations
from typing import Dict
import pandas as pd
from core.Lectura import read_csv_resilient


def _dedupe_factoring(df: pd.DataFrame, fx_cfg: Dict) -> pd.DataFrame:
    """
    Devuelve un DataFrame con columnas:
      PRIORIDAD_NUM (float/int) y FACTORING
    Dedupe por PRIORIDAD_NUM según duplicate_policy.
    """
    if df is None or df.empty:
        return df

    d = df.copy()
    d.columns = [str(c).strip().upper() for c in d.columns]

    if "PRIORIDAD" not in d.columns or "FACTORING" not in d.columns:
        raise ValueError("Factoring: faltan columnas PRIORIDAD y/o FACTORING en el maestro.")

    d["PRIORIDAD_NUM"] = pd.to_numeric(d["PRIORIDAD"], errors="coerce")

    policy = (fx_cfg or {}).get("duplicate_policy", "last_row")
    if policy == "min_prioridad":
        d = d.sort_values(["PRIORIDAD_NUM"]).drop_duplicates(["PRIORIDAD_NUM"], keep="first")
    elif policy == "first_row":
        d = d.drop_duplicates(["PRIORIDAD_NUM"], keep="first")
    else:
        d = d.drop_duplicates(["PRIORIDAD_NUM"], keep="last")

    return d[["PRIORIDAD_NUM", "FACTORING"]]


def load_factoring_from_config(fx_cfg: Dict) -> pd.DataFrame | None:
    """
    Carga maestro de factoring desde Google Sheet publicado como CSV.
    Espera:
      fx_cfg = {
        "enabled": true,
        "source": "google_sheet_csv",
        "url": "https://.../export?format=csv&gid=XXXXX",
        "duplicate_policy": "last_row" | "first_row" | "min_prioridad"
      }
    """
    if not fx_cfg or not fx_cfg.get("enabled"):
        return None
    if (fx_cfg.get("source") or "").lower() != "google_sheet_csv":
        return None

    url = fx_cfg.get("url")
    if not url:
        return None

    df = read_csv_resilient(url)
    if df is None or df.empty:
        return None

    return _dedupe_factoring(df, fx_cfg)


def apply_factoring_lookup(df: pd.DataFrame, fx_cfg: Dict, master_fx: pd.DataFrame) -> pd.DataFrame:
    """
    Une por PRIORIDAD (numérica) y escribe la columna 'factoring' (o la definida en YAML).
    Respeta match_policy:
      on_column, write_to, overwrite_existing, trace_field, trace_value.
    """
    if master_fx is None or master_fx.empty:
        return df

    df = df.copy()

    mp = (fx_cfg or {}).get("match_policy", {})
    on_col     = mp.get("on_column", "prioridad")
    out_col    = mp.get("write_to", "factoring")
    overwrite  = bool(mp.get("overwrite_existing", True))
    trace_field= mp.get("trace_field")
    trace_value= mp.get("trace_value", "MAESTRO_SHEET_FACT")

    # Preparar columna prioridad numérica (lado izquierdo)
    pr_left = pd.to_numeric(df.get(on_col), errors="coerce")
    left = pd.DataFrame({"__idx": df.index, "PRIORIDAD_NUM": pr_left})

    # Hacer merge con el maestro deduplicado
    joined = left.merge(master_fx, on="PRIORIDAD_NUM", how="left")

    if overwrite:
        df[out_col] = joined["FACTORING"].values
    else:
        if out_col not in df.columns:
            df[out_col] = pd.NA
        blank = df[out_col].isna() | (df[out_col].astype("string").str.len() == 0)
        df.loc[blank, out_col] = joined.loc[blank, "FACTORING"].values

    if trace_field:
        has_match = joined["FACTORING"].notna()
        df.loc[has_match, trace_field] = trace_value

    return df
