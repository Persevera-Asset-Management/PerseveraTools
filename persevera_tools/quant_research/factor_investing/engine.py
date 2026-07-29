"""Factor investing backtest orchestration."""

from __future__ import annotations

import pandas as pd

from .config import BacktestConfig
from .data import build_rebalance_dates, densify_prices, load_backtest_panels
from .definitions import (
    get_higher_is_better_map,
    load_factor_definitions,
    resolve_component_names,
)
from .portfolio import scores_to_weights
from .result import BacktestResult, build_summary
from .scoring import calculate_factor_exposure, snapshot_components, snapshot_series


def _prices_wide(price_panel: pd.DataFrame, price_field: str) -> pd.DataFrame:
    """Convert MultiIndex price panel to date × ticker wide frame."""
    if price_panel.empty:
        return pd.DataFrame()
    if isinstance(price_panel.columns, pd.MultiIndex):
        try:
            wide = price_panel.xs(price_field, level=-1, axis=1)
        except KeyError:
            return pd.DataFrame()
        return wide.sort_index()
    return price_panel.sort_index()


def compute_asset_returns(
    prices: pd.DataFrame,
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    ffill_limit: int = 5,
) -> pd.DataFrame:
    """
    Densify prices onto a business-day calendar, forward-fill short gaps, then
    compute simple returns so multi-day moves are not collapsed into one step.
    """
    from .data import densify_prices

    dense = densify_prices(prices, start=start, end=end, ffill_limit=ffill_limit)
    return dense.pct_change(fill_method=None)


def _daily_portfolio_return(weights: pd.Series, asset_returns: pd.Series) -> float:
    """
    Dot product of weights and asset returns, renormalizing among names with
    valid returns.

    Long-only: remaining positive weights are rescaled to sum to the original
    long gross exposure.

    Long-short: long and short legs are rescaled independently to their
    original gross exposures (typically +1 and -1).
    """
    if weights is None or weights.empty:
        return float("nan")

    aligned = pd.concat(
        [weights.rename("w"), asset_returns.rename("r")],
        axis=1,
        join="inner",
    ).dropna()
    if aligned.empty:
        return float("nan")

    w = aligned["w"]
    r = aligned["r"]
    target_long = float(weights[weights > 0].sum())
    target_short = float(weights[weights < 0].sum())

    long_mask = w > 0
    short_mask = w < 0
    w_adj = pd.Series(0.0, index=w.index, dtype=float)

    if long_mask.any() and target_long != 0:
        long_w = w[long_mask]
        w_adj.loc[long_w.index] = long_w / long_w.sum() * target_long

    if short_mask.any() and target_short != 0:
        short_w = w[short_mask]
        w_adj.loc[short_w.index] = short_w / short_w.sum() * target_short

    if not long_mask.any() and not short_mask.any():
        return float("nan")

    return float((w_adj * r).sum())


def run_backtest(config: BacktestConfig) -> BacktestResult:
    """
    Run a factor backtest end-to-end.

    1. Resolve components from Fibery style tags (or explicit mnemonics)
    2. Load historical ``factor_zoo`` panels via ``get_descriptors``
       (all tickers if ``codes`` is None)
    3. On each rebalance date: ADTV filter → PIT snapshot → score → weights
    4. Hold weights until the next rebalance; compute daily P&L from ``price_field``
    """
    definitions = load_factor_definitions()
    components = resolve_component_names(
        style=config.style,
        components=tuple(config.components) if config.components else None,
        definitions=definitions,
    )
    higher_is_better = get_higher_is_better_map(definitions)

    panels = load_backtest_panels(config, components)
    component_panel = panels["components"]
    adtv_panel = panels["adtv"]
    price_panel = panels["prices"]

    prices_raw = _prices_wide(price_panel, config.price_field)
    if prices_raw.empty:
        raise ValueError(f"No price data found for field {config.price_field!r}")

    prices = densify_prices(
        prices_raw,
        start=config.start_date,
        end=config.end_date,
        ffill_limit=config.price_ffill_limit,
    )

    calendar = prices.index
    rebalance_dates = build_rebalance_dates(config, calendar)
    if rebalance_dates.empty:
        raise ValueError("No rebalance dates in the requested window")

    score_rows: list[pd.Series] = []
    weight_rows: list[pd.Series] = []

    for dt in rebalance_dates:
        adtv = snapshot_series(adtv_panel, dt, config.adtv_field)
        if adtv.empty:
            continue
        universe = adtv[adtv >= config.adtv_min].dropna().index
        if len(universe) < config.min_names:
            continue

        cross = snapshot_components(component_panel, dt, components)
        if cross.empty:
            continue
        cross = cross.reindex(universe)
        cross = cross.dropna(how="all")
        if len(cross) < config.min_names:
            continue

        scores = calculate_factor_exposure(
            cross,
            factor_name=config.style or "factor_score",
            higher_is_better_map=higher_is_better,
            zR=config.zR,
            zC=config.zC,
        )
        scores = scores.dropna()
        if len(scores) < config.min_names:
            continue

        weights = scores_to_weights(
            scores,
            construction=config.construction,
            selection_mode=config.selection_mode,
            quantile=config.quantile,
            top_n=config.top_n,
        )
        if weights.empty:
            continue

        score_rows.append(scores.rename(dt))
        weight_rows.append(weights.rename(dt))

    if not weight_rows:
        raise ValueError(
            "No valid rebalance portfolios were formed; "
            "relax adtv_min / min_names or check data coverage"
        )

    scores_df = pd.concat(score_rows, axis=1).T
    scores_df.index = pd.DatetimeIndex(scores_df.index)
    weights_df = pd.concat(weight_rows, axis=1).T.fillna(0.0)
    weights_df.index = pd.DatetimeIndex(weights_df.index)

    trading_index = prices.index[
        (prices.index >= config.start_date) & (prices.index <= config.end_date)
    ]
    daily_weights = weights_df.reindex(trading_index).ffill()
    asset_returns = prices.pct_change(fill_method=None)

    strategy_returns = pd.Series(index=trading_index, dtype=float, name="strategy_return")
    for dt in trading_index:
        # Signal lag = 1 session: return on dt uses weights as of prev_dt.
        pos = trading_index.get_loc(dt)
        if not isinstance(pos, (int,)):
            continue
        if pos == 0:
            strategy_returns.loc[dt] = float("nan")
            continue
        prev_dt = trading_index[pos - 1]
        w = daily_weights.loc[prev_dt]
        if not isinstance(w, pd.Series):
            continue
        w = w[w.notna() & (w != 0.0)]
        if w.empty:
            strategy_returns.loc[dt] = float("nan")
            continue
        r = asset_returns.loc[dt]
        strategy_returns.loc[dt] = _daily_portfolio_return(w, r)

    first_valid = strategy_returns.first_valid_index()
    if first_valid is not None:
        strategy_returns.loc[first_valid:] = strategy_returns.loc[first_valid:].fillna(0.0)
        nav = (
            (1.0 + strategy_returns.loc[first_valid:].fillna(0.0)).cumprod()
            * config.initial_nav
        )
        nav = nav.reindex(trading_index)
    else:
        nav = pd.Series(dtype=float, index=trading_index)
    nav.name = "nav"

    summary = build_summary(nav.dropna(), risk_free_rate=config.risk_free_rate)
    summary.update(
        {
            "construction": config.construction,
            "style": config.style,
            "components": list(components),
            "n_rebals": int(len(weights_df)),
            "selection_mode": config.selection_mode,
            "quantile": config.quantile,
            "top_n": config.top_n,
            "adtv_min": config.adtv_min,
            "denomination": config.denomination,
        }
    )

    return BacktestResult(
        nav=nav,
        returns=strategy_returns,
        weights=weights_df,
        scores=scores_df,
        summary=summary,
        config=config,
        components=components,
    )
