from __future__ import annotations
from pathlib import Path
from typing import Any, Dict
import pandas as pd
import yaml

def load_yaml(p: str | Path) -> Dict[str, Any]:
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def read_source(path: Path, opts: Dict[str, Any]) -> pd.DataFrame:
    if path.suffix.lower() in (".csv", ".txt"):
        return pd.read_csv(
            path, sep=opts.get("sep", ","), decimal=opts.get("decimal", "."),
            encoding=opts.get("encoding"), dtype=str, on_bad_lines="skip",
        )
    if path.suffix.lower() in (".xlsx", ".xls"):
        return pd.read_excel(path, sheet_name=opts.get("sheet", 0), dtype=str)
    return pd.read_csv(path, dtype=str)

def read_csv_resilient(src: str) -> pd.DataFrame:
    return pd.read_csv(src, dtype=str, encoding="utf-8-sig")
