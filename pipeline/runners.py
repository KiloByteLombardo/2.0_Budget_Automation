from __future__ import annotations
import pandas as pd
from pathlib import Path
from typing import Any, Dict, Tuple

from core.Lectura import load_yaml, read_source
from pipeline.normalize import normalize_source
from pipeline.post import apply_post
from core.dtypes import cast_dtypes, to_dt
from lookups.prioridad import load_priorities_from_config, apply_priority_lookup
from lookups.factoring import load_factoring_from_config, apply_factoring_lookup
from lookups.tipo import load_tipo_map_from_config
from pipeline.enrich import grupo_pago_from_prioridad, grupo_pago_from_tienda_sucursal_o_proveedor


def run_mercancia(
    schema_path: str,
    country_path: str,
    ebs_path: str,
    reim_path: str,
    rsf_path: str,
    exec_date: pd.Timestamp | None = None,
) -> Tuple[pd.DataFrame, dict, dict]:
    """Runner unificado para Mercancía (CO/VE).

    Retorna: (df_consolidado_estandar, raw_sources, export_cfg)
    - export_cfg incluye headers/order del país y, si aplica, "__tipo_map".
    """
    country_all = load_yaml(country_path)
    schema = load_yaml(schema_path)["mercancia"]
    cfg = country_all["mercancia"]

    dtypes = schema["dtypes"]
    inputs = cfg.get("inputs", {})

    # Leer crudos
    ebs_df = read_source(Path(ebs_path), inputs.get("ebs", {}))
    reim_df = read_source(Path(reim_path), inputs.get("reim", {}))
    rsf_df = read_source(Path(rsf_path), inputs.get("rsf", {}))
    raw_sources = {"EBS": ebs_df, "REIM": reim_df, "RSF": rsf_df}

    # Normalizar por fuente
    ebs_n = normalize_source(ebs_df, "ebs", cfg, schema); ebs_n["APP"] = "EBS"
    reim_n = normalize_source(reim_df, "reim", cfg, schema); reim_n["APP"] = "REIM"
    rsf_n = normalize_source(rsf_df, "rsf", cfg, schema); rsf_n["APP"] = "RSF"
    base = pd.concat([ebs_n, reim_n, rsf_n], ignore_index=True, sort=False)

    # Fecha creación robusta en EBS
    mask_ebs = base.get("APP", pd.Series("", index=base.index)).eq("EBS")
    if "fecha_creacion" in base.columns:
        fc = pd.to_datetime(base.loc[mask_ebs, "fecha_creacion"], errors="coerce", dayfirst=True)
    else:
        fc = pd.Series(pd.NaT, index=base.index)
    if "fecha" in base.columns:
        alt = pd.to_datetime(base.loc[mask_ebs, "fecha"], errors="coerce", dayfirst=True)
        fc = fc.combine_first(alt)
    base.loc[mask_ebs, "fecha_creacion"] = fc

    # Lookups (prioridades/factoring) declarados bajo mercancia.lookups
    lk_cfg = (cfg.get("lookups", {}) or {})

    pr_cfg = (lk_cfg.get("prioridades", {}) or {})
    if pr_cfg.get("enabled"):
        master = load_priorities_from_config(pr_cfg)
        if master is not None and not master.empty:
            base = apply_priority_lookup(base, pr_cfg, master)

    fx_cfg = (lk_cfg.get("factoring", {}) or {})
    if fx_cfg.get("enabled"):
        master_fx = load_factoring_from_config(fx_cfg)
        if master_fx is not None and not master_fx.empty:
            base = apply_factoring_lookup(base, fx_cfg, master_fx)

    # Mini maestro TIPO a nivel raíz (VE)
    tipo_map = None
    tp_cfg_root = (country_all.get("lookups", {}) or {}).get("tipo_mercancia", {})
    if tp_cfg_root.get("enabled"):
        tipo_map = load_tipo_map_from_config(tp_cfg_root)
        mpc = (tp_cfg_root or {}).get("match_policy_consolidated", {})
        if mpc and mpc.get("enabled") and (tipo_map is not None and not getattr(tipo_map, "empty", True)):
            apply_srcs = set(mpc.get("apply_to_sources", ["EBS", "REIM", "RSF"]))
            on_col = mpc.get("on_column", "proveedor")
            out_col = mpc.get("write_to", "tipo")
            overwrite = bool(mpc.get("overwrite_existing", False))
            trace_f = mpc.get("trace_field")
            trace_val = mpc.get("trace_value", "MAESTRO_TIPO")

            app_col = "APP" if "APP" in base.columns else ("origen" if "origen" in base.columns else None)
            mask_src = base[app_col].astype("string").str.upper().isin({s.upper() for s in apply_srcs}) if app_col else False
            if out_col not in base.columns:
                base[out_col] = pd.NA
            need = mask_src & (overwrite | (base[out_col].isna() | (base[out_col].astype("string").str.len() == 0)))
            if need.any():
                prov = base.loc[need, on_col].astype("string").str.replace("\u00A0", " ", regex=False).str.strip()
                lk = prov.map(tipo_map)
                base.loc[need & lk.notna(), out_col] = lk[lk.notna()]
                if trace_f:
                    base.loc[need & lk.notna(), trace_f] = trace_val

    # Lunes de ejecución
    if exec_date is None:
        exec_date = pd.Timestamp.today().normalize()
    exec_mon = exec_date - pd.to_timedelta(exec_date.weekday(), unit="D")

    # Post (acepta bajo mercancia.post o raíz.post)
    post_cfg = (cfg.get("post", {}) or country_all.get("post", {}) or {})
    base = apply_post(base, post_cfg, context={"exec_mon": exec_mon})

    # Enriquecimientos solicitados para VE en consolidado: Caja y Grupo de Pago
    pais = (cfg.get("const", {}) or {}).get("pais") or (country_all.get("mercancia", {}).get("const", {}) if isinstance(country_all.get("mercancia", {}), dict) else {}).get("pais")

    # Fallback VE (RSF): asegurar fecha_vencimiento = fecha_recepcion + dias_condicion_rms
    if (pais or "").upper() == "VE":
        app_col_fv = "APP" if "APP" in base.columns else None
        if app_col_fv and "fecha_recepcion" in base.columns and "dias_condicion_rms" in base.columns:
            mask_rsf_all = base[app_col_fv].astype("string").str.upper().eq("RSF")
            rec = to_dt(base.loc[mask_rsf_all, "fecha_recepcion"]) if mask_rsf_all.any() else None
            days = pd.to_numeric(base.loc[mask_rsf_all, "dias_condicion_rms"], errors="coerce") if mask_rsf_all.any() else None
            if rec is not None and days is not None:
                fv = rec + pd.to_timedelta(days, unit="D")
                base.loc[mask_rsf_all, "fecha_vencimiento"] = fv.values
    # Caja: calcular desde fecha_vencimiento contra exec_mon
    if "fecha_vencimiento" in base.columns:
        due = pd.to_datetime(base["fecha_vencimiento"], errors="coerce")
        tue = exec_mon + pd.Timedelta(days=1)
        wed = exec_mon + pd.Timedelta(days=2)
        thu = exec_mon + pd.Timedelta(days=3)
        caja = pd.Series("No aplica", index=base.index, dtype="string")
        mask_martes = due.notna() & (due <= tue)
        mask_jueves = due.notna() & (due >= wed) & (due <= thu)
        caja.loc[mask_martes] = "Martes"
        caja.loc[mask_jueves] = "Jueves"
        base["Caja"] = caja

    # Fecha del Documento (VE): EBS/REIM -> 'fecha'; RSF -> 'fecha_recepcion'
    if (pais or "").upper() == "VE":
        app_col_fd = "APP" if "APP" in base.columns else None
        if app_col_fd:
            mask_ebs_fd = base[app_col_fd].astype("string").str.upper().eq("EBS")
            mask_reim_fd = base[app_col_fd].astype("string").str.upper().eq("REIM")
            mask_rsf_fd = base[app_col_fd].astype("string").str.upper().eq("RSF")
            base["fecha_documento"] = pd.NaT
            # EBS: usar exclusivamente 'fecha' (mapeada desde "FECHA DOCUMENTO" en YAML)
            if mask_ebs_fd.any() and "fecha" in base.columns:
                base.loc[mask_ebs_fd, "fecha_documento"] = to_dt(base.loc[mask_ebs_fd, "fecha"])  

            # REIM: prefer 'fecha' (Fecha Factura). Fallback: fecha_recepcion -> fecha_creacion
            if mask_reim_fd.any():
                reim_fd = pd.Series(pd.NaT, index=base.index)
                if "fecha" in base.columns:
                    reim_fd = reim_fd.combine_first(to_dt(base.loc[mask_reim_fd, "fecha"]))
                if "fecha_recepcion" in base.columns:
                    reim_fd = reim_fd.combine_first(to_dt(base.loc[mask_reim_fd, "fecha_recepcion"]))
                if "fecha_creacion" in base.columns:
                    reim_fd = reim_fd.combine_first(to_dt(base.loc[mask_reim_fd, "fecha_creacion"]))
                base.loc[mask_reim_fd, "fecha_documento"] = reim_fd.loc[mask_reim_fd]

            # RSF: prefer 'fecha_recepcion'. Fallback: fecha_vencimiento
            if mask_rsf_fd.any():
                rsf_fd = pd.Series(pd.NaT, index=base.index)
                if "fecha_recepcion" in base.columns:
                    rsf_fd = rsf_fd.combine_first(to_dt(base.loc[mask_rsf_fd, "fecha_recepcion"]))
                if "fecha_vencimiento" in base.columns:
                    rsf_fd = rsf_fd.combine_first(to_dt(base.loc[mask_rsf_fd, "fecha_vencimiento"]))
                base.loc[mask_rsf_fd, "fecha_documento"] = rsf_fd.loc[mask_rsf_fd]

            # Corrección robusta EBS: si fecha_documento quedó NaT, usar el valor CRUDO 'FECHA DOCUMENTO' por DOCUMENTO
            if mask_ebs_fd.any() and raw_sources.get("EBS") is not None:
                raw_ebs = raw_sources["EBS"]
                col_doc = "DOCUMENTO" if "DOCUMENTO" in raw_ebs.columns else None
                col_fd  = "FECHA DOCUMENTO" if "FECHA DOCUMENTO" in raw_ebs.columns else None
                if col_doc and col_fd:
                    doc_key = raw_ebs[col_doc].astype("string").str.replace("\u00A0"," ", regex=False).str.strip()
                    fd_src  = to_dt(raw_ebs[col_fd])
                    # Dedupe por DOCUMENTO para evitar InvalidIndexError (usar la primera coincidencia)
                    mapping_series = pd.Series(fd_src.values, index=doc_key.values)
                    mapping_series = mapping_series[~mapping_series.index.duplicated(keep="first")]
                    mapping_dict = mapping_series.to_dict()
                    target_docs = base.loc[mask_ebs_fd, "factura"].astype("string").str.replace("\u00A0"," ", regex=False).str.strip()
                    mapped = target_docs.map(mapping_dict)
                    need = mask_ebs_fd & (base["fecha_documento"].isna()) & mapped.notna()
                    base.loc[need, "fecha_documento"] = mapped.loc[need[need].index].values

    # Grupo de Pago: EBS por prioridad; REIM/RSF por reglas + mini maestro
    app_col = "APP" if "APP" in base.columns else None
    if app_col:
        mask_ebs = base[app_col].astype("string").str.upper().eq("EBS")
        mask_reim = base[app_col].astype("string").str.upper().eq("REIM")
        mask_rsf = base[app_col].astype("string").str.upper().eq("RSF")

        # inicializar columna
        if "Grupo de Pago" not in base.columns:
            base["Grupo de Pago"] = pd.NA

        # EBS -> mapear prioridad
        if "prioridad" in base.columns:
            base.loc[mask_ebs, "Grupo de Pago"] = base.loc[mask_ebs, "prioridad"].apply(grupo_pago_from_prioridad)

        # REIM/RSF -> reglas con columnas normalizadas
        tienda_col = "tienda_nombre" if "tienda_nombre" in base.columns else ("tienda" if "tienda" in base.columns else None)
        suc_col = "sucursal_proveedor" if "sucursal_proveedor" in base.columns else ("sucursal" if "sucursal" in base.columns else None)
        prov_col = "proveedor" if "proveedor" in base.columns else None
        if tienda_col and prov_col:
            if mask_reim.any():
                base.loc[mask_reim, "Grupo de Pago"] = grupo_pago_from_tienda_sucursal_o_proveedor(
                    base.loc[mask_reim], tienda_col=tienda_col, sucursal_col=suc_col or "sucursal_proveedor", proveedor_col=prov_col, tipo_map=tipo_map
                ).values
            if mask_rsf.any():
                base.loc[mask_rsf, "Grupo de Pago"] = grupo_pago_from_tienda_sucursal_o_proveedor(
                    base.loc[mask_rsf], tienda_col=tienda_col, sucursal_col=suc_col or "sucursal_proveedor", proveedor_col=prov_col, tipo_map=tipo_map
                ).values

    # Forzar tipo_documento STANDARD para RSF (VE)
    if app_col:
        mask_rsf_all = base[app_col].astype("string").str.upper().eq("RSF")
        if mask_rsf_all.any():
            base.loc[mask_rsf_all, "tipo_documento"] = "STANDARD"

    # Fallback VE: calcular 'monto' si faltó en post (neto o bruto)
    if ("monto" not in base.columns) or base["monto"].isna().all():
        base["monto"] = pd.to_numeric(base.get("monto_neto"), errors="coerce").fillna(
            pd.to_numeric(base.get("monto_bruto"), errors="coerce")
        )

    # Filtro general del consolidado: excluir 0 <= monto <= 100
    if "monto" in base.columns:
        _m = pd.to_numeric(base["monto"], errors="coerce")
        base = base[_m.isna() | (_m < 0) | (_m > 100)].copy()

    # Filtro consolidado VE: conservar solo Caja en valores permitidos (configurable por YAML)
    if (pais or "").upper() == "VE" and "Caja" in base.columns:
        cfg_export = (cfg.get("export") or country_all.get("export") or {})
        caja_allowed = cfg_export.get("filter_caja_values", ["Martes", "Jueves"]) or ["Martes", "Jueves"]
        base = base[base["Caja"].isin(caja_allowed)].copy()

    # Filtro consolidado VE: conservar solo Grupo de Pago permitido (configurable por YAML)
    if (pais or "").upper() == "VE" and "Grupo de Pago" in base.columns:
        cfg_export = (cfg.get("export") or country_all.get("export") or {})
        gp_allowed_conf = cfg_export.get("filter_grupo_pago_values")
        gp_allowed = {s.upper() for s in (gp_allowed_conf or ["DIRECTO", "ALMACEN", "PPV RMS", "SUMINISTROS"]) }
        gp_norm = base["Grupo de Pago"].astype("string").str.upper()
        base = base[gp_norm.isin(gp_allowed)].copy()

    # Para Colombia: no incluir columna calculada 'Grupo de Pago' en el consolidado
    if (pais or "").upper() == "CO" and "Grupo de Pago" in base.columns:
        base.drop(columns=["Grupo de Pago"], inplace=True)

    # Tipado y orden estándar por schema
    base = cast_dtypes(base, dtypes)
    order = schema.get("order", [])
    final_cols = [c for c in order if c in base.columns]
    # Asegurar columnas VE necesarias en consolidado
    if (pais or "").upper() == "VE":
        for extra in ["fecha_documento", "Grupo de Pago", "Caja"]:
            if extra in base.columns and extra not in final_cols:
                final_cols.append(extra)
    out = base[final_cols] if final_cols else base

    # Export config (mercancia.export o raíz.export)
    export_cfg = (cfg.get("export") or country_all.get("export") or {})
    # Override de export para Venezuela: columnas solicitadas
    if (pais or "").upper() == "VE":
        export_cfg = dict(export_cfg)  # copia superficial
        headers = dict(export_cfg.get("headers", {}))
        headers.update({
            "factura": "Numero de Factura",
            "orden_compra": "Orden De Compra",
            "proveedor": "Proveedor",
            "fecha_documento": "Fecha del documento",
            "monto": "Monto",
            "APP": "APP",
        })
        export_cfg["headers"] = headers
        export_cfg["order"] = [
            "APP",
            "Grupo de Pago",
            "Proveedor",
            "Numero de Factura",
            "Orden De Compra",
            "Fecha del documento",
            "Monto",
            "Caja",
        ]
    export_cfg["__tipo_map"] = tipo_map
    # Bandera de país para export y políticas de RAW
    export_cfg["__pais"] = (pais or "").upper() if pais else None
    if (pais or "").upper() == "CO":
        # Colombia: escribir RAW sin enriquecer, y filtrar RSF a Recepción sin factura
        export_cfg = dict(export_cfg)
        export_cfg["write_sources_raw"] = True
        export_cfg["enrich_raw_sources"] = False
    return out, raw_sources, export_cfg


def run_colombia_mercancia(
    schema_path: str,
    country_path: str,
    ebs_path: str,
    reim_path: str,
    rsf_path: str,
    exec_date: pd.Timestamp | None = None,
) -> Tuple[pd.DataFrame, dict, dict]:
    return run_mercancia(schema_path, country_path, ebs_path, reim_path, rsf_path, exec_date)


def run_venezuela_mercancia(
    schema_path: str,
    country_path: str,
    ebs_path: str,
    reim_path: str,
    rsf_path: str,
    exec_date: pd.Timestamp | None = None,
) -> Tuple[pd.DataFrame, dict, dict]:
    return run_mercancia(schema_path, country_path, ebs_path, reim_path, rsf_path, exec_date)
