from __future__ import annotations
from typing import Any, Dict
import pandas as pd
from core.dtypes import to_dt

def apply_post(df: pd.DataFrame, post_cfg: Dict[str, Any], context: Dict[str, Any] | None = None) -> pd.DataFrame:
    if not post_cfg: return df
    env = {"pd": pd, "to_dt": to_dt}
    if context: env.update(context)
    for stmt in post_cfg.get("compute", []):
        exec(stmt, env, {"df": df})
    return df
