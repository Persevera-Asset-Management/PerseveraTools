import pandas as pd
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.seasonal import STL

# res = STL(time_series, period=12).fit()
# res.plot()
# plt.show()
# res.trend

def seasonal_adjust(
    time_series: pd.Series | pd.DataFrame,
    model: str = "additive",
    period: int | None = None,
) -> pd.Series | pd.DataFrame:
    """
    Adjusts a time series for seasonality using seasonal decomposition.

    Args:
        time_series: A pandas Series or DataFrame with a DatetimeIndex.
        model: The model to use for decomposition, either "additive" or
            "multiplicative".
        period: The seasonal period of the time series. If None, it will be
            inferred from the index.

    Returns:
        A pandas Series or DataFrame with the seasonally adjusted data.
    """
    if isinstance(time_series, pd.DataFrame):
        return time_series.apply(
            seasonal_adjust, model=model, period=period
        )

    result = seasonal_decompose(time_series, model=model, period=period)

    if model == "additive":
        return time_series - result.seasonal

    return time_series / result.seasonal


def decompose_time_series(
    time_series: pd.Series | pd.DataFrame,
    model: str = "additive",
    period: int | None = None,
) -> pd.DataFrame:
    """
    Decomposes a time series into trend, seasonal, and residual components.

    Args:
        time_series: A pandas Series or DataFrame with a DatetimeIndex.
        model: The model to use for decomposition, either "additive" or
            "multiplicative".
        period: The seasonal period of the time series. If None, it will be
            inferred from the index.

    Returns:
        A pandas DataFrame with the trend, seasonal, and residual components.
    """
    if isinstance(time_series, pd.DataFrame):
        return time_series.apply(
            decompose_time_series, model=model, period=period
        )

    result = seasonal_decompose(time_series, model=model, period=period)
    return pd.DataFrame(
        {
            "trend": result.trend,
            "seasonal": result.seasonal,
            "residual": result.resid,
        }
    )

