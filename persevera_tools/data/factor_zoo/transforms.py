"""Pure transforms: factor_zoo long-format input -> derived long-format output."""

from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd

from .helpers import daily_ffilled_panel


def _stack_wide(panel: pd.DataFrame) -> pd.Series:
    # pandas>=2.1: future_stack without dropna; 1.x: dropna=False only
    try:
        return panel.stack(future_stack=True)
    except TypeError:
        return panel.stack(dropna=False)


def _panel_to_long(
    panel: pd.DataFrame,
    observed_dates: pd.DatetimeIndex,
    field_label: str,
) -> pd.DataFrame:
    r = panel.reindex(observed_dates)
    stacked = _stack_wide(r)
    stacked.name = "value"
    out = stacked.reset_index()
    out.columns = ["date", "code", "value"]
    out["field"] = field_label
    return out[["code", "date", "field", "value"]]


def compute_price_momentum(data: pd.DataFrame) -> pd.DataFrame:
    temp_pivot = data.pivot(index="date", columns="code", values="value")
    temp, orig_idx = daily_ffilled_panel(temp_pivot, ffill_limit=21)
    chunks: List[pd.DataFrame] = []
    for horizon in [1, 3, 6, 9, 12]:
        shifted = temp.pct_change(periods=1, freq=f"{30 * horizon}D").multiply(100)
        chunks.append(_panel_to_long(shifted, orig_idx, f"momentum_{horizon}m"))

    for horizon in [3, 6, 9, 12]:
        shifted = (
            temp.pct_change(periods=1, freq=f"{30 * (horizon - 1)}D")
            .shift(30)
            .multiply(100)
        )
        chunks.append(_panel_to_long(shifted, orig_idx, f"momentum_{horizon}m1"))

    for days in [7, 14]:
        shifted = temp.pct_change(periods=1, freq=f"{days}D").multiply(100)
        chunks.append(_panel_to_long(shifted, orig_idx, f"momentum_{days}d"))

    return pd.concat(chunks, ignore_index=True)


def compute_price_range(data: pd.DataFrame) -> pd.DataFrame:
    temp_pivot = data.pivot(index="date", columns="code", values="value")
    temp, orig_idx = daily_ffilled_panel(temp_pivot, ffill_limit=21)
    lo = temp.rolling(window=360).min()
    hi = temp.rolling(window=360).max()
    denom = hi - lo
    r_arr = np.where(denom.abs() > 1e-12, (temp - lo) / denom * 100.0, np.nan)
    r = pd.DataFrame(r_arr, index=temp.index, columns=temp.columns)
    return _panel_to_long(r, orig_idx, "price_range")


def compute_short_selling(data: pd.DataFrame) -> pd.DataFrame:
    temp = data.pivot(index=["code", "date"], columns="field", values="value")
    r = temp.eval(
        """
        short_interest_pct = short_interest / shares_outstanding / 1e4
        days_to_cover = short_interest / median_volume_traded_21d
        """
    )
    out = r[["short_interest_pct", "days_to_cover"]]
    stacked = _stack_wide(out)
    stacked.name = "value"
    long_df = stacked.reset_index()
    long_df.columns = ["code", "date", "field", "value"]
    return long_df


def compute_liquidity(data: pd.DataFrame) -> pd.DataFrame:
    field_names = sorted(data["field"].dropna().unique().tolist())
    temp = data.pivot(index=["field", "date"], columns="code", values="value")

    chunks: List[pd.DataFrame] = []
    for fld in field_names:
        fld_panel = temp.loc[fld]
        for window in [7, 14, 21, 63, 252]:
            rolled = fld_panel.rolling(window=window, min_periods=1).median()
            name = (
                f"median_volume_traded_{window}d"
                if fld == "num_shares_traded"
                else f"median_dollar_volume_traded_{window}d"
            )
            stacked = _stack_wide(rolled)
            stacked.name = "value"
            one = stacked.reset_index()
            one.columns = ["date", "code", "value"]
            one["field"] = name
            chunks.append(one[["code", "date", "field", "value"]])

    merged = pd.concat(chunks, ignore_index=True)

    wide = merged.pivot(index=["code", "date"], columns="field", values="value")
    for window in [7, 14, 21]:
        pair = wide.eval(
            f"""
            delta_dollar_volume_{window}d_63d = median_dollar_volume_traded_{window}d \
                / median_dollar_volume_traded_63d
            delta_volume_{window}d_63d = median_volume_traded_{window}d \
                / median_volume_traded_63d
            """
        )
        sub_cols = [
            f"delta_dollar_volume_{window}d_63d",
            f"delta_volume_{window}d_63d",
        ]
        sub = pair[sub_cols]
        stacked = _stack_wide(sub)
        stacked.name = "value"
        extra = stacked.reset_index()
        extra.columns = ["code", "date", "field", "value"]
        merged = pd.concat([merged, extra], ignore_index=True)

    return merged


def compute_ratios_growth(data: pd.DataFrame) -> pd.DataFrame:
    temp_pivot = data.pivot(index=["field", "date"], columns="code", values="value")
    uniq_fields = sorted(data["field"].dropna().unique().tolist())

    chunks: List[pd.DataFrame] = []
    for fld in uniq_fields:
        temp_field = temp_pivot.loc[fld]
        densified, obs_idx = daily_ffilled_panel(temp_field, ffill_limit=360)
        shifted = densified.diff(360).reindex(obs_idx)
        stacked = _stack_wide(shifted)
        stacked.name = "value"
        blk = stacked.reset_index()
        blk.columns = ["date", "code", "value"]
        blk["field"] = fld
        chunks.append(blk[["code", "date", "field", "value"]])

    growth_long = pd.concat(chunks, ignore_index=True)

    enriched = pd.merge(
        data, growth_long, on=["code", "date", "field"], how="left"
    ).dropna()
    enriched = enriched.drop(columns=["value_x"])
    enriched = enriched.rename(columns={"value_y": "value"})
    enriched["field"] = enriched["field"].apply(lambda x: f"{x}_growth_1y")
    return enriched[["code", "date", "field", "value"]]


def compute_value(data: pd.DataFrame) -> pd.DataFrame:
    temp = data.pivot(index=["code", "date"], columns="field", values="value")
    carry = ["ebitda_ltm", "ebit_ltm", "total_equity"]
    available = [c for c in carry if c in temp.columns]
    if available:
        temp[available] = temp.groupby(level=0)[available].transform(
            lambda df: df.ffill(limit=252)
        )
    r = temp.eval(
        """
        earnings_yield_fwd = earnings_per_share_fwd / price_close
        book_yield_fwd = book_value_per_share_fwd / price_close
        book_yield_ltm = total_equity / market_cap
        fcf_yield_fwd = free_cash_flow_fwd / ev
        ebit_yield_fwd = ebit_fwd / ev
        ebitda_yield_fwd = ebitda_fwd / ev
        ebitda_yield_ltm = ebitda_ltm / ev
        ebit_yield_ltm = ebit_ltm / ev
        """
    ).multiply(100)
    keep = [
        "earnings_yield_fwd",
        "book_yield_fwd",
        "book_yield_ltm",
        "fcf_yield_fwd",
        "ebit_yield_fwd",
        "ebitda_yield_fwd",
        "ebitda_yield_ltm",
        "ebit_yield_ltm",
    ]
    out = r[keep]
    stacked = _stack_wide(out)
    stacked.name = "value"
    long_df = stacked.reset_index()
    long_df.columns = ["code", "date", "field", "value"]
    return long_df


# ── Quality variability helpers ───────────────────────────────────────────────

def _trend_se(arr: np.ndarray) -> float:
    """Standard error of residuals from OLS ŷ = α + β·t (ddof=2).

    Measures instability *after* removing a linear trend, so a company whose
    margin improves steadily is not penalised.  Equivalent to the MSCI Quality
    Index 'earnings variability' method when applied to quarterly snapshots.

    Returns NaN when fewer than 4 non-NaN observations are present.
    """
    mask = ~np.isnan(arr)
    n = int(mask.sum())
    if n < 4:
        return np.nan
    y = arr[mask]
    t = np.arange(n, dtype=np.float64)
    t_c = t - t.mean()
    ss_t = float(np.dot(t_c, t_c))
    if ss_t < 1e-12:
        return np.nan
    b = np.dot(t_c, y) / ss_t
    resid = y - (y.mean() + b * t_c)
    # SS_resid / (n - 2) = SE_regression²
    return float(np.sqrt(np.dot(resid, resid) / max(n - 2, 1)))


def _downside_std(arr: np.ndarray) -> float:
    """Square root of the mean squared negative deviation from the mean.

    Only penalises downside surprises.  Useful when upside variance is benign
    (e.g. margin beats) but downside instability is what matters for quality.

    Returns NaN when fewer than 4 non-NaN observations are present.
    """
    valid = arr[~np.isnan(arr)]
    if len(valid) < 4:
        return np.nan
    mu = valid.mean()
    neg = np.minimum(valid - mu, 0.0)
    return float(np.sqrt(np.mean(neg * neg)))


# ── Quality variability ───────────────────────────────────────────────────────

# Fields on which variability metrics are computed.  Include the growth field
# so that revenue_growth_variability is produced when this function is called
# AFTER compute_ratios_growth has added net_revenues_ltm_growth_1y to data.
_VARIABILITY_FIELDS = [
    "gross_margin",
    "ebitda_margin",
    "ebit_margin",
    "fcf_margin",
    "net_margin",
    "roe",
    "roa",
    "roic",
    "roce",
    "net_revenues_ltm_growth_1y",   # requires compute_ratios_growth upstream
]

# Suffix → rolling function factory
_VARIABILITY_VARIANTS = {
    "variability":          lambda w, m: {"func": "std",     "window": w, "min_periods": m},
    "trend_deviation":      None,   # handled separately via .apply(_trend_se)
    "downside_variability": None,   # handled separately via .apply(_downside_std)
}


def compute_quality_variability(
    data: pd.DataFrame,
    *,
    rolling_window_years: int = 5,
    min_obs_quarters: int = 8,
) -> pd.DataFrame:
    """Rolling variability metrics for Quality factor (three variants per field).

    Variants produced for each field in ``_VARIABILITY_FIELDS``:

    ``{field}_variability``
        Rolling standard deviation (σ).  Simple and fast.  Equivalent to the
        AQR *Quality Minus Junk* approach.

    ``{field}_trend_deviation``
        Standard error of OLS residuals from ŷ = α + β·t fitted on each
        rolling window.  Does not penalise steady secular improvement.
        Closest to the MSCI Quality Index methodology.

    ``{field}_downside_variability``
        Square root of mean squared *negative* deviations from the period
        mean.  Penalises downside surprises only; benign upside variance is
        ignored.

    Parameters
    ----------
    rolling_window_years:
        Look-back in years (converted to ``rolling_window_years × 360`` daily
        rows on the forward-filled panel, matching the convention used in
        ``compute_value_timeseries``).
    min_obs_quarters:
        Minimum number of quarterly observations required before a value is
        emitted (converted to ``min_obs_quarters × 90`` daily rows).

    Pipeline note
    -------------
    To obtain ``net_revenues_ltm_growth_1y_variability`` (revenue growth
    variability), this function must be called *after*
    ``compute_ratios_growth`` has been applied to the data so that the
    ``net_revenues_ltm_growth_1y`` field is present.  Fields absent from
    ``data`` are silently skipped.
    """
    window = rolling_window_years * 360
    min_periods = min_obs_quarters * 90

    avail = set(data["field"].unique())
    fields_to_run = [f for f in _VARIABILITY_FIELDS if f in avail]
    if not fields_to_run:
        return pd.DataFrame(columns=["code", "date", "field", "value"])

    temp_pivot = data[data["field"].isin(fields_to_run)].pivot(
        index=["field", "date"], columns="code", values="value"
    )

    chunks: List[pd.DataFrame] = []
    for fld in fields_to_run:
        fld_panel = temp_pivot.loc[fld]
        # ffill_limit=90: carry quarterly snapshot forward for ≤1 quarter
        densified, obs_idx = daily_ffilled_panel(fld_panel, ffill_limit=90)

        # 1. Simple rolling σ
        rolled_std = densified.rolling(window=window, min_periods=min_periods).std()
        out_name = (
            "revenue_growth_variability"
            if fld == "net_revenues_ltm_growth_1y"
            else f"{fld}_variability"
        )
        chunks.append(_panel_to_long(rolled_std, obs_idx, out_name))

        # 2. Trend deviation (SE of OLS residuals) — via rolling .apply
        rolled_td = densified.rolling(
            window=window, min_periods=min_periods
        ).apply(_trend_se, raw=True)
        td_name = (
            "revenue_growth_trend_deviation"
            if fld == "net_revenues_ltm_growth_1y"
            else f"{fld}_trend_deviation"
        )
        chunks.append(_panel_to_long(rolled_td, obs_idx, td_name))

        # 3. Downside semi-standard deviation
        rolled_ds = densified.rolling(
            window=window, min_periods=min_periods
        ).apply(_downside_std, raw=True)
        ds_name = (
            "revenue_growth_downside_variability"
            if fld == "net_revenues_ltm_growth_1y"
            else f"{fld}_downside_variability"
        )
        chunks.append(_panel_to_long(rolled_ds, obs_idx, ds_name))

    return pd.concat(chunks, ignore_index=True)


# ── Accruals ──────────────────────────────────────────────────────────────────

def compute_accruals(data: pd.DataFrame) -> pd.DataFrame:
    """Accruals ratio — earnings quality signal (Sloan 1996).

    Produces two variants when the required fields are present:

    ``accruals_ratio_bs``  *(balance-sheet method)*
        ``Δ(total_equity + net_debt) / avg(total_assets)``

        Change in Net Operating Assets (NOA) scaled by average total assets.
        NOA is approximated as ``total_equity + net_debt``, which equals
        total assets minus cash and financial liabilities — a standard proxy
        that requires only balance-sheet fields already in the database.

        Annual Δ is computed as a 360-day diff on the forward-filled daily
        panel, matching the convention used in ``compute_ratios_growth``.

    ``accruals_ratio_cf``  *(cash-flow method, full Sloan formula)*
        ``(net_income_q − CFO_q − CFI_q) / total_assets``

        Requires ``cash_flow_from_investing_activities_q``, which becomes
        available after downloading the new CF fields.  This is the
        *complete* version: subtracting CFI removes the capex component
        that the simplified ``(NI − CFO)`` approximation ignores.

        Expressed at the quarterly level (not annualised) to match the
        reporting frequency of the underlying fields.

    ``capex_to_sales``  *(proxy via CFI)*
        ``−cash_flow_from_investing_activities_q / net_revenues_q``

        Capex intensity proxy.  CFI is used as a proxy for capex; note that
        CFI also includes acquisitions and asset disposals, so this metric
        overstates true capex intensity for companies with active M&A.

    Both ratio variants are expressed in percent (×100) and follow the
    long-format ``(code, date, field, value)`` convention.

    A *higher* accruals ratio signals lower earnings quality (earnings are
    driven by accruals rather than cash flows).  Inversion to produce a
    "quality" signal (higher = better) should be applied at the scoring layer.
    """
    chunks: List[pd.DataFrame] = []

    # ── Pivot to (code, date) × field ────────────────────────────────────────
    wide = data.pivot(index=["code", "date"], columns="field", values="value")

    # Forward-fill balance-sheet items: quarterly reporting → daily panel
    bs_carry = ["total_equity", "net_debt", "total_assets"]
    available_carry = [c for c in bs_carry if c in wide.columns]
    if available_carry:
        wide[available_carry] = wide.groupby(level=0)[available_carry].transform(
            lambda df: df.ffill(limit=252)
        )

    # ── 1. Balance-sheet accruals ─────────────────────────────────────────────
    bs_needed = {"total_equity", "net_debt", "total_assets"}
    if bs_needed.issubset(wide.columns):
        # NOA = total_equity + net_debt  →  (code, date) Series
        noa_series = wide["total_equity"] + wide["net_debt"]

        # Convert to (date × code) panels for daily_ffilled_panel
        noa_panel = noa_series.unstack(level=0)          # date rows, code cols
        assets_panel = wide["total_assets"].unstack(level=0)

        noa_dense, obs_idx = daily_ffilled_panel(noa_panel, ffill_limit=90)
        assets_dense, _ = daily_ffilled_panel(assets_panel, ffill_limit=90)

        # Annual Δ NOA (360-day diff matching compute_ratios_growth convention)
        delta_noa = noa_dense.diff(360)

        # Average total assets over the same 360-day window
        avg_assets = (assets_dense + assets_dense.shift(360)) / 2

        accruals_bs = (delta_noa / avg_assets.replace(0, np.nan) * 100).reindex(obs_idx)
        chunks.append(_panel_to_long(accruals_bs, obs_idx, "accruals_ratio_bs"))

    # ── 2. Cash-flow accruals (full Sloan) ────────────────────────────────────
    cf_needed = {
        "net_income_q",
        "cash_flow_from_operations_q",
        "cash_flow_from_investing_activities_q",
        "total_assets",
    }
    if cf_needed.issubset(wide.columns):
        cf_result = (
            wide["net_income_q"]
            - wide["cash_flow_from_operations_q"]
            - wide["cash_flow_from_investing_activities_q"]
        ) / wide["total_assets"].replace(0, np.nan) * 100

        stacked = cf_result.rename("accruals_ratio_cf")
        cf_long = stacked.reset_index()
        cf_long.columns = ["code", "date", "value"]
        cf_long["field"] = "accruals_ratio_cf"
        chunks.append(cf_long[["code", "date", "field", "value"]])

    # ── 3. Capex-to-sales proxy ───────────────────────────────────────────────
    capex_needed = {"cash_flow_from_investing_activities_q", "net_revenues_q"}
    if capex_needed.issubset(wide.columns):
        capex_ratio = (
            -wide["cash_flow_from_investing_activities_q"]
            / wide["net_revenues_q"].replace(0, np.nan)
            * 100
        )
        stacked_c = capex_ratio.rename("capex_to_sales")
        cap_long = stacked_c.reset_index()
        cap_long.columns = ["code", "date", "value"]
        cap_long["field"] = "capex_to_sales"
        chunks.append(cap_long[["code", "date", "field", "value"]])

    return (
        pd.concat(chunks, ignore_index=True)
        if chunks
        else pd.DataFrame(columns=["code", "date", "field", "value"])
    )


# ── Operating leverage ────────────────────────────────────────────────────────

def compute_operating_leverage(
    data: pd.DataFrame,
    *,
    rolling_quarters: int = 8,
    min_obs_quarters: int = 4,
    clip_dol: float = 20.0,
    min_rev_change_pct: float = 1.0,
) -> pd.DataFrame:
    """Degree of Operating Leverage (DOL) as a Quality/Safety signal.

    ``operating_leverage`` = rolling median of ``%Δ EBIT_q / %Δ Revenue_q``

    A company with high DOL has margins that amplify the business cycle:
    revenue drops translate into outsized EBIT declines.  This is a
    *"false quality" filter*: high-margin businesses with high DOL look
    attractive on static profitability screens but are fragile in downturns.

    Implementation
    --------------
    1.  Both ``ebit_q`` and ``net_revenues_q`` are forward-filled for at most
        100 days (within-quarter carry) to align the quarterly reporting
        cadence to a daily panel.
    2.  Quarter-over-quarter percentage changes are computed as 90-day diffs
        on the daily panel.
    3.  Revenue changes below ``min_rev_change_pct``% in absolute value are
        masked (NaN) to avoid noise-driven outliers when revenue is nearly
        flat — a small EBIT move divided by a near-zero revenue move would
        produce a spurious and very large DOL.
    4.  Raw DOL observations are clipped to ``[−clip_dol, +clip_dol]`` before
        taking the rolling median, further dampening outlier influence.
    5.  A rolling median over ``rolling_quarters × 90`` days is taken as the
        final estimate.

    Parameters
    ----------
    rolling_quarters:
        Number of quarterly observations in the rolling window (default 8 = 2
        years).
    min_obs_quarters:
        Minimum non-NaN quarterly observations required to emit a value.
    clip_dol:
        Absolute cap on individual quarterly DOL observations before rolling.
    min_rev_change_pct:
        Minimum absolute revenue QoQ change (in %) below which the
        observation is treated as NaN.
    """
    needed = {"ebit_q", "net_revenues_q"}
    if not needed.issubset(set(data["field"].unique())):
        return pd.DataFrame(columns=["code", "date", "field", "value"])

    temp_pivot = data[data["field"].isin(needed)].pivot(
        index=["field", "date"], columns="code", values="value"
    )

    ebit_panel = temp_pivot.loc["ebit_q"]
    rev_panel = temp_pivot.loc["net_revenues_q"]

    ebit_dense, obs_idx = daily_ffilled_panel(ebit_panel, ffill_limit=100)
    rev_dense, _ = daily_ffilled_panel(rev_panel, ffill_limit=100)

    # Align panels to the same date index
    common_idx = ebit_dense.index.intersection(rev_dense.index)
    ebit_dense = ebit_dense.reindex(common_idx)
    rev_dense = rev_dense.reindex(common_idx)

    # QoQ pct change via 90-day diff on the forward-filled daily panel
    rev_prev = rev_dense.shift(90)
    ebit_prev = ebit_dense.shift(90)

    rev_pct = (rev_dense - rev_prev) / rev_prev.abs().replace(0, np.nan) * 100
    ebit_pct = (ebit_dense - ebit_prev) / ebit_prev.abs().replace(0, np.nan) * 100

    # Mask near-zero revenue changes to avoid noise-driven extremes
    rev_safe = rev_pct.where(rev_pct.abs() >= min_rev_change_pct)

    # Raw DOL: clip before rolling to dampen single-quarter outliers
    dol_raw = (ebit_pct / rev_safe).clip(-clip_dol, clip_dol)

    # Rolling median over rolling_quarters quarters
    window = rolling_quarters * 90
    min_periods = min_obs_quarters * 90
    dol_rolling = dol_raw.rolling(window=window, min_periods=min_periods).median()

    return _panel_to_long(dol_rolling, obs_idx, "operating_leverage")


def compute_value_timeseries(
    data: pd.DataFrame,
    *,
    rolling_window_years: int = 10,
) -> pd.DataFrame:
    temp_pivot = data.pivot(index=["field", "date"], columns="code", values="value")
    uniq_fields = sorted(data["field"].dropna().unique().tolist())

    chunks: List[pd.DataFrame] = []
    horizon = rolling_window_years * 360
    for fld in uniq_fields:
        temp_field = temp_pivot.loc[fld]
        densified, obs_idx = daily_ffilled_panel(temp_field, ffill_limit=360)

        pct_rank = densified.apply(
            lambda series: pd.Series(series).rolling(
                window=horizon, min_periods=360
            ).rank(pct=True),
            axis=0,
            raw=False,
        )
        pct_rank = pct_rank.reindex(obs_idx).multiply(100)
        stacked = _stack_wide(pct_rank)
        stacked.name = "value"
        blk = stacked.reset_index()
        blk.columns = ["date", "code", "value"]
        base = fld[:-4] if len(fld) > 4 else fld
        blk["field"] = f"{base}_percentile_{rolling_window_years}y"
        chunks.append(blk[["code", "date", "field", "value"]])

    return pd.concat(chunks, ignore_index=True)