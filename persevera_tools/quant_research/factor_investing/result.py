"""Backtest result container."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd

from ..metrics import (
    calculate_annualized_return,
    calculate_annualized_volatility,
    calculate_max_drawdown,
    calculate_sharpe_ratio,
)


@dataclass
class BacktestResult:
    """Outputs of ``run_backtest``."""

    nav: pd.Series
    returns: pd.Series
    weights: pd.DataFrame
    scores: pd.DataFrame
    summary: dict[str, Any] = field(default_factory=dict)
    config: Optional[Any] = None
    components: tuple[str, ...] = ()

    def __repr__(self) -> str:
        n = len(self.returns.dropna())
        sharpe = self.summary.get("sharpe")
        ann = self.summary.get("annualized_return")
        return (
            f"BacktestResult(n_obs={n}, "
            f"ann_return={ann!r}, sharpe={sharpe!r})"
        )


def build_summary(
    nav: pd.Series,
    risk_free_rate: float = 0.0,
) -> dict[str, Any]:
    """Compute standard performance stats from a NAV series (price proxy)."""
    clean = nav.dropna()
    if clean.empty:
        return {
            "annualized_return": float("nan"),
            "annualized_volatility": float("nan"),
            "max_drawdown": float("nan"),
            "sharpe": float("nan"),
            "total_return": float("nan"),
            "n_obs": 0,
        }

    total_return = float(clean.iloc[-1] / clean.iloc[0] - 1.0)
    return {
        "annualized_return": calculate_annualized_return(clean),
        "annualized_volatility": calculate_annualized_volatility(clean, frequency="daily"),
        "max_drawdown": calculate_max_drawdown(clean),
        "sharpe": calculate_sharpe_ratio(clean, risk_free_rate=risk_free_rate),
        "total_return": total_return,
        "n_obs": int(len(clean.pct_change(fill_method=None).dropna())),
    }
