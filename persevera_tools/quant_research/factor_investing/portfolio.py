"""Portfolio construction from cross-sectional factor scores."""

from __future__ import annotations

from typing import Literal, Optional

import numpy as np
import pandas as pd

Construction = Literal["long_only", "long_short"]
SelectionMode = Literal["quantile", "top_n"]


def _tail_count(
    n: int,
    *,
    selection_mode: SelectionMode = "quantile",
    quantile: float = 0.2,
    top_n: Optional[int] = None,
) -> int:
    """Number of names in each tail; at least 1 when n >= 1."""
    if n <= 0:
        return 0
    if selection_mode == "top_n":
        if top_n is None or top_n < 1:
            raise ValueError("top_n must be >= 1 when selection_mode='top_n'")
        return min(int(top_n), n)
    k = int(np.floor(n * quantile))
    return max(k, 1)


def scores_to_weights(
    scores: pd.Series,
    construction: Construction = "long_only",
    *,
    selection_mode: SelectionMode = "quantile",
    quantile: float = 0.2,
    top_n: Optional[int] = None,
) -> pd.Series:
    """
    Map a cross-sectional score Series to portfolio weights.

    Selection:
      - ``quantile``: take fraction ``quantile`` of ranked names per tail
      - ``top_n``: take a fixed ``top_n`` names per tail

    Construction:
      - ``long_only``: equal-weight the top tail
      - ``long_short``: equal-weight long top tail and short bottom tail
        (dollar-neutral: long gross = short gross = 1)
    """
    clean = scores.dropna().sort_values(ascending=False)
    if clean.empty:
        return pd.Series(dtype=float)

    n = len(clean)
    k = _tail_count(
        n,
        selection_mode=selection_mode,
        quantile=quantile,
        top_n=top_n,
    )
    if k <= 0:
        return pd.Series(dtype=float)

    weights = pd.Series(0.0, index=clean.index, dtype=float)

    if construction == "long_only":
        longs = clean.index[:k]
        weights.loc[longs] = 1.0 / len(longs)
        return weights[weights != 0.0]

    if construction == "long_short":
        longs = clean.index[:k]
        shorts = clean.index[-k:]
        overlap = set(longs).intersection(shorts)
        if overlap:
            mid = n // 2
            if mid == 0:
                return pd.Series(dtype=float)
            longs = clean.index[:mid]
            shorts = clean.index[mid:]
            if len(longs) == 0 or len(shorts) == 0:
                return pd.Series(dtype=float)
        weights.loc[longs] = 1.0 / len(longs)
        weights.loc[shorts] = -1.0 / len(shorts)
        return weights[weights != 0.0]

    raise ValueError(f"Unknown construction: {construction!r}")
