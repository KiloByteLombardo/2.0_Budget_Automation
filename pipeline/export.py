from __future__ import annotations
import pandas as pd
from core.utils import sanitize_sheet_name
from .enrich import enrich_raw_sources


def apply_headers_and_order(df: pd.DataFrame, export_cfg: dict) -> pd.DataFrame:
    out = df.copy()
    headers_map = (export_cfg or {}).get("headers", {})
    order = (export_cfg or {}).get("order")
    if headers_map:
        out = out.rename(columns=headers_map)
    if order:
        keep = [c for c in order if c in out.columns]
        out = out[keep]
    return out


def write_excel_with_raw(
    out_path: str,
    consolidated_df: pd.DataFrame,
    export_cfg: dict,
    raw_sources: dict[str, pd.DataFrame] | None = None,
    exec_mon: pd.Timestamp | None = None,
    tipo_map: pd.Series | None = None,
):
    sheets = (export_cfg or {}).get("sheets", {}) or {}
    s_cons = sheets.get("consolidated", "Consolidado")
    s_ebs = sheets.get("ebs_raw", "EBS (Original)")
    s_reim = sheets.get("reim_raw", "REIM (Original)")
    s_rsf = sheets.get("rsf_raw", "RSF (Original)")

    if (export_cfg or {}).get("filter_consolidated_by_en_alcance", False) and (
        "en_alcance" in consolidated_df.columns
    ):
        consolidated_df = consolidated_df.loc[consolidated_df["en_alcance"] == True].copy()

    df_cons = apply_headers_and_order(consolidated_df, export_cfg)

    write_raw = (
        (bool((export_cfg or {}).get("write_sources_raw", False)) or ("__tipo_map" in (export_cfg or {})))
        and (raw_sources is not None)
    )
    # Enriquecer RAW solo si la configuración lo permite (VE sí; CO no)
    enrich_flag = bool((export_cfg or {}).get("enrich_raw_sources", True))
    enriched = (
        enrich_raw_sources(raw_sources, exec_mon, tipo_map=tipo_map)
        if (write_raw and exec_mon is not None and enrich_flag)
        else (raw_sources or {})
    )

    used: set[str] = set()

    def uniq(n: str) -> str:
        base = sanitize_sheet_name(n)
        name = base
        i = 1
        while name.upper() in used:
            suf = f"_{i}"
            name = sanitize_sheet_name(base[: 31 - len(suf)] + suf)
            i += 1
        used.add(name.upper())
        return name

    s_cons = uniq(s_cons)
    if write_raw:
        s_ebs = uniq(s_ebs)
        s_reim = uniq(s_reim)
        s_rsf = uniq(s_rsf)

    add_gp_formula = (export_cfg or {}).get("add_grupo_pago_formula_xl")
    if add_gp_formula is None:
        add_gp_formula = write_raw and ("__tipo_map" in (export_cfg or {}))
    engine = "xlsxwriter" if (write_raw and add_gp_formula) else "openpyxl"

    with pd.ExcelWriter(out_path, engine=engine) as xw:
        df_cons.to_excel(xw, index=False, sheet_name=s_cons)
        if write_raw:
            to_write = dict(enriched)
            # Colombia: filtrar RSF a 'Recepción sin factura'
            if (export_cfg or {}).get("__pais") == "CO" and to_write.get("RSF") is not None:
                df = to_write["RSF"].copy()
                col_est = None
                for c in ["Estatus", "ESTATUS"]:
                    if c in df.columns:
                        col_est = c; break
                if col_est:
                    s = df[col_est].astype("string")
                    # normalizar mínimamente tildes comunes
                    for a, b in [("Ó","O"),("Á","A"),("É","E"),("Í","I"),("Ú","U"),("Ñ","N")]:
                        s = s.str.replace(a, b, regex=False)
                    mask = s.str.upper().eq("RECEPCION SIN FACTURA")
                    to_write["RSF"] = df.loc[mask].copy()
            if "EBS" in to_write and to_write["EBS"] is not None:
                to_write["EBS"].to_excel(xw, index=False, sheet_name=s_ebs)
            if "REIM" in to_write and to_write["REIM"] is not None:
                to_write["REIM"].to_excel(xw, index=False, sheet_name=s_reim)
            if "RSF" in to_write and to_write["RSF"] is not None:
                to_write["RSF"].to_excel(xw, index=False, sheet_name=s_rsf)

        if write_raw and add_gp_formula and engine == "xlsxwriter":
            # Create AUX sheet from mini-master if present
            s_aux = uniq("AUX")
            try:
                tm = (export_cfg or {}).get("__tipo_map")
                if tm is not None and not getattr(tm, "empty", True):
                    aux_df = pd.DataFrame({"Proveedor": tm.index.astype("string"), "TIPO": tm.astype("string").values})
                    aux_df.to_excel(xw, index=False, sheet_name=s_aux)
            except Exception:
                pass

            def col_to_letter(cidx: int) -> str:
                s = ""
                c = cidx
                while True:
                    c, r = divmod(c, 26)
                    s = chr(65 + r) + s
                    if c == 0:
                        break
                    c -= 1
                return s

            def add_formula(sheet_name: str, df: pd.DataFrame):
                if df is None or df.empty:
                    return
                ws = xw.sheets.get(sheet_name)
                if ws is None:
                    return
                cols = list(df.columns)
                if "Tienda" not in cols:
                    return
                suc_name = (
                    "Sucursal Proveedor" if "Sucursal Proveedor" in cols else ("Sucursal" if "Sucursal" in cols else None)
                )
                if suc_name is None:
                    return
                tienda_idx = cols.index("Tienda")
                suc_idx = cols.index(suc_name)
                prov_idx = cols.index("Proveedor") if "Proveedor" in cols else None
                nrows = len(df)
                gp_col_idx = len(cols)
                ws.write(0, gp_col_idx, "Grupo de Pago (XL)")
                t_col = col_to_letter(tienda_idx)
                s_col = col_to_letter(suc_idx)
                aux_range = f"'{s_aux}'!$A:$B"
                for i in range(nrows):
                    row = i + 2
                    t_cell = f"${t_col}{row}"
                    s_cell = f"${s_col}{row}"
                    if prov_idx is not None:
                        p_col = col_to_letter(prov_idx)
                        p_cell = f"${p_col}{row}"
                        vlookup = (
                            f"IFERROR(VLOOKUP({s_cell},{aux_range},2,FALSE),IFERROR(VLOOKUP({p_cell},{aux_range},2,FALSE),\"NO DEFINIDO\"))"
                        )
                    else:
                        vlookup = f"IFERROR(VLOOKUP({s_cell},{aux_range},2,FALSE),\"NO DEFINIDO\")"
                    formula = (
                        f"=IF({t_cell}<>\"CENDIS\",\"DIRECTO\",IF(OR(RIGHT({s_cell},3)=\"PPV\",RIGHT({s_cell},4)=\"PPV1\",RIGHT({s_cell},4)=\"PPV2\",RIGHT({s_cell},4)=\"PPV3\"),\"PPV RMS\",{vlookup}))"
                    )
                    ws.write_formula(i + 1, gp_col_idx, formula)

            if s_reim in xw.sheets:
                add_formula(s_reim, enriched.get("REIM"))
            if s_rsf in xw.sheets:
                add_formula(s_rsf, enriched.get("RSF"))
