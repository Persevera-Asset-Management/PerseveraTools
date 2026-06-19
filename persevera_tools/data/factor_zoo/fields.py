"""Resolve input mnemonics for derived factor pipelines from Fibery config."""


from __future__ import annotations

from typing import List

import pandas as pd

from ...db.fibery import read_fibery

_FIBERY_TABLE = "Inv-Rsrch-Quant/Definições dos Fatores"
_PRICE_DERIVED_CATEGORIES = frozenset({"price_momentum", "price_range"})
_PRICE_INPUT_FIELDS = ("price_close",)

# Fibery may tag the raw LTM field; the variability transform consumes growth.
_LOAD_FIELD_REPLACEMENTS: dict[str, dict[str, str]] = {
    "quality_variability": {"net_revenues_ltm": "net_revenues_ltm_growth_1y"},
}

_factor_definitions_cache: pd.DataFrame | None = None


def clear_factor_definitions_cache() -> None:
    """Drop cached Fibery rows (use between batch runs or in tests)."""
    global _factor_definitions_cache
    _factor_definitions_cache = None


def preload_factor_definitions() -> pd.DataFrame:
    """Load active factor definitions from Fibery once and retain in memory."""
    global _factor_definitions_cache
    if _factor_definitions_cache is None:
        df = read_fibery(table_name=_FIBERY_TABLE)
        if "state" in df.columns:
            df = df[df["state"] == "Ativo"]
        _factor_definitions_cache = df
    return _factor_definitions_cache


def _dependent_categories(val: object) -> list[str]:
    """Normalize ``Categoria Dependente`` (scalar or multi-select list)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return []
    if isinstance(val, list):
        return [
            str(x)
            for x in val
            if x is not None and str(x) not in ("<NA>", "nan", "None")
        ]
    s = str(val).strip()
    return [] if s in ("", "<NA>", "NAN", "NONE") else [s]


def get_fields_for_category(category: str) -> List[str]:
    """
    List input mnemonics for a derived pipeline category from Fibery
    ``Inv-Rsrch-Quant/Definições dos Fatores`` (``Categoria Dependente``).

    Supports multi-select ``Categoria Dependente`` (list of category slugs).
    ``price_momentum`` and ``price_range`` always consume ``price_close``.

    Raises
    ------
    ValueError
        If no active rows exist for ``category``.
    """
    if category in _PRICE_DERIVED_CATEGORIES:
        return list(_PRICE_INPUT_FIELDS)

    df = preload_factor_definitions()
    mask = df["Categoria Dependente"].apply(lambda v: category in _dependent_categories(v))
    subset = df.loc[mask]

    if subset.empty:
        raise ValueError(
            f"No mnemonics in Definições dos Fatores for category {category!r}. "
            "Ensure category is listed in Categoria Dependente for at least one active row."
        )

    names = sorted({str(x) for x in subset["Name"].tolist()})
    replacements = _LOAD_FIELD_REPLACEMENTS.get(category, {})
    if replacements:
        names = sorted({replacements.get(n, n) for n in names})
    return names


def sanitize_sql_in_literals(values: List[str]) -> str:
    """Build a PostgreSQL-safe ``IN (...)`` literal list from mnemonic strings."""
    parts = ["'" + v.replace("'", "''") + "'" for v in values]
    return "(" + ", ".join(parts) + ")"
