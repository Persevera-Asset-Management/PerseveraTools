"""Load factor_zoo slices, derive fields, optionally upsert."""


from __future__ import annotations

import logging
from typing import Dict, Iterable, Literal, Mapping, Sequence

import pandas as pd

from ...db.operations import read_sql, to_sql
from .fields import (
    clear_factor_definitions_cache,
    get_fields_for_category,
    preload_factor_definitions,
    sanitize_sql_in_literals,
)
from .transforms import (
    compute_accruals,
    compute_liquidity,
    compute_operating_leverage,
    compute_price_momentum,
    compute_price_range,
    compute_quality_variability,
    compute_ratios_growth,
    compute_short_selling,
    compute_value,
    compute_value_timeseries,
)

DerivedCategory = Literal[
    "price_momentum",
    "price_range",
    "liquidity",
    "ratios_growth",
    "value",
    "accruals",
    "operating_leverage",
    "value_timeseries",
    "short_selling",
    "quality_variability",
]

DERIVED_INDEPENDENT_ORDER: tuple[DerivedCategory, ...] = (
    "price_momentum",
    "price_range",
    "liquidity",
    "ratios_growth",
    "value",
    "accruals",
    "operating_leverage",
)

DERIVED_DEPENDENT_ORDER: tuple[DerivedCategory, ...] = (
    "value_timeseries",
    "short_selling",
    "quality_variability",
)

logger = logging.getLogger(__name__)

_VALID_DERIVED: frozenset[str] = frozenset(DERIVED_INDEPENDENT_ORDER + DERIVED_DEPENDENT_ORDER)


def _run_transform(chunk: pd.DataFrame, category: str, extras: Mapping[str, object]) -> pd.DataFrame:
    if category == "price_momentum":
        return compute_price_momentum(chunk)
    if category == "price_range":
        return compute_price_range(chunk)
    if category == "liquidity":
        return compute_liquidity(chunk)
    if category == "ratios_growth":
        return compute_ratios_growth(chunk)
    if category == "value":
        return compute_value(chunk)
    if category == "accruals":
        return compute_accruals(chunk)
    if category == "operating_leverage":
        return compute_operating_leverage(
            chunk,
            rolling_quarters=int(extras.get("rolling_quarters", 8)),
            min_obs_quarters=int(extras.get("min_obs_quarters", 4)),
            clip_dol=float(extras.get("clip_dol", 20.0)),
            min_rev_change_pct=float(extras.get("min_rev_change_pct", 1.0)),
        )
    if category == "value_timeseries":
        return compute_value_timeseries(
            chunk,
            rolling_window_years=int(extras.get("rolling_window_years", 10)),
        )
    if category == "short_selling":
        return compute_short_selling(chunk)
    if category == "quality_variability":
        return compute_quality_variability(
            chunk,
            rolling_window_years=int(extras.get("rolling_window_years", 5)),
            min_obs_quarters=int(extras.get("min_obs_quarters", 8)),
        )
    raise ValueError(f"Unsupported derived category: {category}")


def load_factor_slice(
    fields: Sequence[str], *, sql_min_date: str = "2000-01-01"
) -> pd.DataFrame:
    if not fields:
        return pd.DataFrame(columns=["code", "date", "field", "value"])
    in_clause = sanitize_sql_in_literals(sorted(set(fields)))
    query = (
        "SELECT code, date, field, value FROM factor_zoo "
        f"WHERE field IN {in_clause} AND date > :md "
        "ORDER BY date, field, code"
    )
    return read_sql(query, params={"md": sql_min_date}, date_columns=["date"])


def finalize_derived(df: pd.DataFrame, *, output_min_date: str | None = "2024-01-01") -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = df.sort_values(["date", "field", "code"]).copy()
    out["value"] = out["value"].round(5)
    if output_min_date:
        thresh = pd.Timestamp(output_min_date)
        out = out[out["date"] > thresh]
    return out[["code", "date", "field", "value"]]


def process_category(
    category: DerivedCategory | str,
    *,
    fields: Sequence[str] | None = None,
    sql_min_date: str = "2000-01-01",
    output_min_date: str | None = "2024-01-01",
    upload: bool = True,
    table_name: str = "factor_zoo",
    primary_keys: Sequence[str] = ("code", "date", "field"),
    batch_size: int = 5000,
    compute_extras: Mapping[str, object] | None = None,
) -> pd.DataFrame:
    """
    Read ``factor_zoo`` rows for mnemonics configured in Fibery
    ``Definições dos Fatores``, compute derived fields for ``category``, optionally upsert.
    """
    cat = str(category)
    if cat not in _VALID_DERIVED:
        raise ValueError(f"Unknown category {cat!r}. Expected one of {sorted(_VALID_DERIVED)}.")

    if fields is None:
        fields_seq = get_fields_for_category(cat)
    else:
        fields_seq = list(fields)

    chunk = load_factor_slice(fields_seq, sql_min_date=sql_min_date)
    if chunk.empty:
        logger.warning(
            "No factor_zoo rows for category=%s (sample fields %s…) after sql date filter %s",
            cat,
            fields_seq[:10],
            sql_min_date,
        )
        return chunk

    extras = compute_extras or {}
    derived = _run_transform(chunk, cat, extras)
    finalized = finalize_derived(derived, output_min_date=output_min_date)

    if upload and not finalized.empty:
        to_sql(
            finalized.reset_index(drop=True),
            table_name=table_name,
            primary_keys=list(primary_keys),
            update=True,
            batch_size=batch_size,
        )
    elif upload and finalized.empty:
        logger.info("Skip upload — no rows remain after derivation/filters (%s)", cat)

    return finalized


def process_categories_in_order(
    categories: Iterable[DerivedCategory | str],
    *,
    sql_min_date: str = "2000-01-01",
    output_min_date: str | None = "2024-01-01",
    upload: bool = True,
    compute_extras: Mapping[str, object] | None = None,
) -> Dict[str, pd.DataFrame]:
    """Run categories sequentially."""
    preload_factor_definitions()
    outs: Dict[str, pd.DataFrame] = {}
    for cat in categories:
        outs[str(cat)] = process_category(
            cat,
            sql_min_date=sql_min_date,
            output_min_date=output_min_date,
            upload=upload,
            compute_extras=compute_extras,
        )
    return outs


def run_independent_derived_factors(
    *,
    sql_min_date: str = "2000-01-01",
    output_min_date: str | None = "2024-01-01",
    upload: bool = True,
    compute_extras: Mapping[str, object] | None = None,
) -> Dict[str, pd.DataFrame]:
    """First-stage categories."""
    return process_categories_in_order(
        DERIVED_INDEPENDENT_ORDER,
        sql_min_date=sql_min_date,
        output_min_date=output_min_date,
        upload=upload,
        compute_extras=compute_extras,
    )


def run_dependent_derived_factors(
    *,
    sql_min_date: str = "2000-01-01",
    output_min_date: str | None = "2024-01-01",
    upload: bool = True,
    compute_extras: Mapping[str, object] | None = None,
) -> Dict[str, pd.DataFrame]:
    """Categories that consume earlier derived mnemonics."""
    return process_categories_in_order(
        DERIVED_DEPENDENT_ORDER,
        sql_min_date=sql_min_date,
        output_min_date=output_min_date,
        upload=upload,
        compute_extras=compute_extras,
    )


def run_all_derived_factors_sequentially(
    *,
    sql_min_date: str = "2000-01-01",
    output_min_date: str | None = "2024-01-01",
    upload: bool = True,
    compute_extras: Mapping[str, object] | None = None,
) -> Dict[str, pd.DataFrame]:
    """Runs independent phases then dependents."""
    clear_factor_definitions_cache()
    try:
        out = run_independent_derived_factors(
            sql_min_date=sql_min_date,
            output_min_date=output_min_date,
            upload=upload,
            compute_extras=compute_extras,
        )
        out.update(
            run_dependent_derived_factors(
                sql_min_date=sql_min_date,
                output_min_date=output_min_date,
                upload=upload,
                compute_extras=compute_extras,
            )
        )
        return out
    finally:
        clear_factor_definitions_cache()


def _main(argv: Sequence[str] | None = None) -> None:
    import argparse

    p = argparse.ArgumentParser(
        description="Derive factor_zoo analytics from persisted Bloomberg snapshots."
    )
    p.add_argument(
        "--category",
        choices=sorted(_VALID_DERIVED),
        help="Run a single derived category.",
    )
    p.add_argument(
        "--phase",
        choices=("independent", "dependent", "all"),
        help='Batch preset (ignored if --category is used). Defaults to "all".',
    )
    p.add_argument("--no-upload", action="store_true")
    p.add_argument("--sql-from", dest="sql_min_date", default="2000-01-01")
    p.add_argument(
        "--output-from",
        dest="output_min_date",
        default="2024-01-01",
        help='Truncate output rows strictly after this ISO date (empty string disables trimming).',
    )
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO)

    out_min: str | None = args.output_min_date or None
    base_kw = dict(
        sql_min_date=args.sql_min_date,
        output_min_date=out_min,
        upload=not args.no_upload,
    )

    if args.category:
        process_category(args.category, **base_kw)
        return

    phase = args.phase or "all"
    if phase == "independent":
        run_independent_derived_factors(**base_kw)
    elif phase == "dependent":
        run_dependent_derived_factors(**base_kw)
    else:
        run_all_derived_factors_sequentially(**base_kw)
