from __future__ import annotations
import pandas as pd
from core.dtypes import to_dt

def enrich_raw_sources(raws: dict[str, pd.DataFrame], exec_mon: pd.Timestamp, tipo_map: pd.Series | None = None) -> dict[str, pd.DataFrame]:
    """
    Agrega columnas solicitadas en hojas originales:
      EBS:  Saldo, Caja, Grupo de Pago (desde PRIORIDAD)
      REIM: Caja, Grupo de Pago (si hay PRIORIDAD, si no NO DEFINIDO)
      RSF:  Fecha Vencimiento Verdadero, Caja, Grupo de Pago (si hay PRIORIDAD, si no NO DEFINIDO)
    """
    if not raws:
        return raws

    out = {}
    for key, df in (raws or {}).items():
        if df is None or df.empty:
            out[key] = df
            continue

        d = df.copy()

        if key.upper() == "EBS":
            # --- Saldo (usa 'MONTO A PAGAR') ---
            if "MONTO A PAGAR" in d.columns:
                d["Saldo"] = _saldo_sign_from_amount(d["MONTO A PAGAR"])
            else:
                d["Saldo"] = pd.NA

            # --- Caja (usa 'FECHA A PAGAR') ---
            if "FECHA A PAGAR" in d.columns:
                d["Caja"] = _compute_caja(d["FECHA A PAGAR"], exec_mon)
            else:
                d["Caja"] = pd.NA

            # --- Grupo de Pago (usa 'PRIORIDAD') ---
            if "PRIORIDAD" in d.columns:
                d["Grupo de Pago"] = d["PRIORIDAD"].apply(grupo_pago_from_prioridad)
            else:
                d["Grupo de Pago"] = "NO DEFINIDO"

        elif key.upper() == "REIM":
            # Caja
            col_due = "Fecha Vencimiento"
            d["Caja"] = _compute_caja(d[col_due], exec_mon) if col_due in d.columns else pd.NA

            # Grupo de pago usando tienda/sucursal/proveedor + mini maestro
            tienda_col = "Tienda" if "Tienda" in d.columns else None
            suc_col    = "Sucursal" if "Sucursal" in d.columns else None
            prov_col   = "Proveedor" if "Proveedor" in d.columns else None
            if tienda_col and (suc_col or prov_col):
                d["Grupo de Pago"] = grupo_pago_from_tienda_sucursal_o_proveedor(
                    d,
                    tienda_col=tienda_col,
                    sucursal_col=suc_col or "Sucursal",
                    proveedor_col=prov_col or "Proveedor",
                    tipo_map=tipo_map   # <<< mini maestro PROVEEDOR->TIPO
                )
            else:
                d["Grupo de Pago"] = "NO DEFINIDO"

        elif key.upper() == "RSF":
            # Fecha Vencimiento Verdadero
            col_recv = "Fecha Recepción"; col_days = "Días Condición (RMS)"
            if col_recv in d.columns and col_days in d.columns:
                recv = to_dt(d[col_recv]); days = pd.to_numeric(d[col_days], errors="coerce")
                d["Fecha Vencimiento Verdadero"] = recv + pd.to_timedelta(days, unit="D")
            else:
                d["Fecha Vencimiento Verdadero"] = pd.NaT

            # Caja
            d["Caja"] = _compute_caja(d["Fecha Vencimiento Verdadero"], exec_mon)

            # Grupo de pago usando tienda/sucursal proveedor + mini maestro
            tienda_col = "Tienda" if "Tienda" in d.columns else None
            # en RSF la "sucursal" que suele venir es "Sucursal Proveedor"
            suc_col    = "Sucursal Proveedor" if "Sucursal Proveedor" in d.columns else ("Sucursal" if "Sucursal" in d.columns else None)
            prov_col   = "Proveedor" if "Proveedor" in d.columns else None
            if tienda_col and (suc_col or prov_col):
                d["Grupo de Pago"] = grupo_pago_from_tienda_sucursal_o_proveedor(
                    d,
                    tienda_col=tienda_col,
                    sucursal_col=suc_col or "Sucursal",
                    proveedor_col=prov_col or "Proveedor",
                    tipo_map=tipo_map   # <<< mini maestro PROVEEDOR->TIPO
                )
            else:
                d["Grupo de Pago"] = "NO DEFINIDO"


        out[key] = d

    return out

def grupo_pago_from_prioridad(prio_val) -> str:
    """
    Mapea prioridad (int/str) a Grupo de Pago:
      7 -> ALMACEN
      8 -> SUMINISTROS
      12 -> PPV EBS
      13 -> PPV RMS
      22 -> DIRECTO
      otro/NaN -> NO DEFINIDO
    """
    pr = pd.to_numeric(pd.Series([prio_val]), errors="coerce").iloc[0]
    if pd.isna(pr):
        return "NO DEFINIDO"
    pr = int(pr)
    if pr == 7:  return "ALMACEN"
    if pr == 8:  return "SUMINISTROS"
    if pr == 12: return "PPV EBS"
    if pr == 13: return "PPV RMS"
    if pr == 22: return "DIRECTO"
    return "NO DEFINIDO"

def grupo_pago_from_tienda_sucursal_o_proveedor(df: pd.DataFrame,
                                                tienda_col: str,
                                                sucursal_col: str,
                                                proveedor_col: str,
                                                tipo_map: pd.Series | None) -> pd.Series:
    """
    Regla:
      - Si Tienda != 'CENDIS' -> 'DIRECTO'
      - Else si Sucursal termina en PPV/PPV1/PPV2/PPV3 -> 'PPV RMS'
      - Else si PROVEEDOR está en mini maestro -> usar TIPO del maestro
      - Si no, 'NO DEFINIDO'
    """
    tienda = df.get(tienda_col)
    suc    = df.get(sucursal_col)
    prov   = df.get(proveedor_col)

    if tienda is None:
        return pd.Series("NO DEFINIDO", index=df.index, dtype="string")

    st_tienda = tienda.astype("string").str.strip()
    st_suc    = suc.astype("string").str.strip() if suc is not None else pd.Series(pd.NA, index=df.index, dtype="string")
    st_prov   = prov.astype("string").str.replace("\u00A0"," ", regex=False).str.strip() if prov is not None else pd.Series(pd.NA, index=df.index, dtype="string")

    out = pd.Series("NO DEFINIDO", index=df.index, dtype="string")

    # 1) DIRECTO si tienda != CENDIS
    mask_directo = st_tienda.str.upper() != "CENDIS"
    out.loc[mask_directo] = "DIRECTO"

    # 2) PPV RMS si sucursal termina con PPV*
    suf = st_suc.str.upper().fillna("")
    mask_ppv = suf.str.endswith("PPV") | suf.str.endswith("PPV1") | suf.str.endswith("PPV2") | suf.str.endswith("PPV3")
    mask_cendis = ~mask_directo
    out.loc[mask_cendis & mask_ppv] = "PPV RMS"

    # 3) Fallback por PROVEEDOR en mini maestro (solo los que siguen bajo CENDIS y no PPV*)
    mask_needs_lookup = mask_cendis & (~mask_ppv)
    if tipo_map is not None and not tipo_map.empty:
        # index del map es proveedor literal con strip; hacemos match literal
        out.loc[mask_needs_lookup] = out.loc[mask_needs_lookup]  # no-op para mantener índice
        lk = st_prov.map(tipo_map)
        out.loc[mask_needs_lookup & lk.notna()] = lk[mask_needs_lookup & lk.notna()].astype("string")

    return out

def _compute_caja(due_dates: pd.Series, exec_mon: pd.Timestamp) -> pd.Series:
    """
    Caja para CO: 
      - Si fecha a pagar <= martes de la semana en curso -> 'Martes'
      - Si está entre miércoles y jueves (inclusive) -> 'Jueves'
      - Resto -> 'No aplica'
    """
    due = to_dt(due_dates)
    tue = exec_mon + pd.Timedelta(days=1)
    wed = exec_mon + pd.Timedelta(days=2)
    thu = exec_mon + pd.Timedelta(days=3)

    out = pd.Series("No aplica", index=due.index, dtype="string")
    mask_martes = due.notna() & (due <= tue)
    mask_jueves = due.notna() & (due >= wed) & (due <= thu)
    out.loc[mask_martes] = "Martes"
    out.loc[mask_jueves] = "Jueves"
    return out

def _saldo_sign_from_amount(s: pd.Series) -> pd.Series:
    """
    Si MONTO A PAGAR > 0 => 'Positivo', si <= 0 => 'Negativo'.
    (Se usa texto tal como pediste)
    """
    x = pd.to_numeric(s, errors="coerce").fillna(0)
    return pd.Series(["Positivo" if v > 0 else "Negativo" for v in x], index=s.index, dtype="string")