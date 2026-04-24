import pandas as pd
import numpy as np


def calculate_sqn(close_prices: pd.Series, period: int = 100) -> pd.Series:
    """
    Calculates the System Quality Number (SQN) indicator from a given series of close prices.

    The formula is based on a TradingView script:
    close_difference = close/close[1]-1
    Stdev = stdev(close_difference, Period)
    _sma = sma(close_difference, Period)
    SQN = ((_sma * sqrt(Period)) / Stdev)

    Args:
        close_prices: A pandas Series containing the close prices.
        period: The lookback period for the calculation. Defaults to 100.

    Returns:
        A pandas Series containing the SQN values.
    """
    close_prices = close_prices.dropna()
    close_difference = (close_prices / close_prices.shift(1)) - 1
    sma_close_difference = close_difference.rolling(window=period).mean()
    stdev_close_difference = close_difference.rolling(window=period).std(ddof=0)

    sqn = (sma_close_difference * np.sqrt(period)) / stdev_close_difference

    return sqn

def calculate_sqn_categories(sqn: pd.Series) -> pd.Series:
    """
    Categorizes SQN values based on the TradingView script's color logic.

    The categories correspond to the following hex color codes from the script:
    - Strong Bearish: #800000 (SQN < -1.7)
    - Bearish: #FF0000 (-1.7 <= SQN < -0.6)
    - Neutral: #FFD700 (-0.6 <= SQN <= 0.6)
    - Bullish: #008000 (0.6 < SQN <= 1.7)
    - Strong Bullish: #0000FF (SQN > 1.7)

    Args:
        sqn: A pandas Series containing the SQN values.

    Returns:
        A pandas Series containing the category for each SQN value.
    """
    conditions = [
        sqn < -1.7,
        (sqn >= -1.7) & (sqn < -0.6),
        (sqn >= -0.6) & (sqn <= 0.6),
        (sqn > 0.6) & (sqn <= 1.7),
        sqn > 1.7
    ]
    categories = [
        "Strong Bearish",
        "Bearish",
        "Neutral",
        "Bullish",
        "Strong Bullish"
    ]
    return pd.Series(np.select(conditions, categories, default=np.nan), index=sqn.index)

def calculate_annualized_return(close_prices: pd.Series) -> float:
    """
    Computes the annualized return from a series of close prices.

    Args:
        close_prices: Daily close prices series.

    Returns:
        Annualized return as a float, or NaN if the series is empty.
    """
    returns = close_prices.pct_change().dropna()
    n = len(returns)
    if n == 0:
        return np.nan
    return float((1 + returns).prod() ** (252 / n) - 1)

def calculate_consistency(close_prices: pd.Series, benchmark: pd.Series = None) -> float:
    """
    Computes the fraction of months with positive returns.

    If a benchmark is provided, computes the fraction of months in which
    the series outperformed the benchmark (i.e. excess return > 0).

    Args:
        close_prices: Daily close prices series.
        benchmark: Optional daily close prices series used as benchmark.

    Returns:
        Fraction of positive (or outperforming) months as a float, or NaN if no monthly data.
    """
    monthly = close_prices.resample("ME").last().pct_change().dropna()
    if len(monthly) == 0:
        return np.nan
    if benchmark is not None:
        monthly_bm = benchmark.resample("ME").last().pct_change().dropna()
        aligned = pd.concat([monthly, monthly_bm], axis=1, join='inner').dropna()
        if len(aligned) == 0:
            return np.nan
        excess = aligned.iloc[:, 0] - aligned.iloc[:, 1]
        return float((excess > 0).mean())
    return float((monthly > 0).mean())

def calculate_annualized_volatility(close_prices: pd.Series, frequency: str = 'weekly') -> float:
    """
    Computes the annualized volatility from a series of close prices.

    Args:
        close_prices: Daily close prices series.
        frequency: Resampling frequency for return calculation. One of 'daily',
            'weekly' (default), 'monthly', or 'yearly'.

    Returns:
        Annualized standard deviation as a float.
    """
    freq_map = {
        'daily':   ('D',  252),
        'weekly':  ('W',   52),
        'monthly': ('ME',  12),
        'yearly':  ('YE',   1),
    }
    if frequency not in freq_map:
        raise ValueError(f"Invalid frequency: {frequency}. Choose from {list(freq_map)}")
    resample_rule, days_scale = freq_map[frequency]
    returns = close_prices.resample(resample_rule).last().pct_change().dropna()
    return float(returns.std() * np.sqrt(days_scale))

def calculate_max_drawdown(close_prices: pd.Series) -> float:
    """
    Computes the maximum drawdown from a series of close prices.

    Args:
        close_prices: Daily close prices series.

    Returns:
        Maximum drawdown as a negative float (e.g. -0.15 for -15%).
    """
    return float((close_prices / close_prices.cummax() - 1).min())

def calculate_sharpe_ratio(close_prices: pd.Series, risk_free_rate: float) -> float:
    """
    Computes the Sharpe ratio.

    Args:
        close_prices: Daily close prices series.
        risk_free_rate: Annualized risk-free rate (e.g. 0.10 for 10%).

    Returns:
        Sharpe ratio as a float, or NaN if volatility is zero.
    """
    vol = calculate_annualized_volatility(close_prices, frequency='daily')
    return (calculate_annualized_return(close_prices) - risk_free_rate) / vol if vol > 0 else np.nan

def calculate_sortino_ratio(close_prices: pd.Series, risk_free_rate: float) -> float:
    """
    Computes the Sortino ratio using downside deviation as the risk measure.

    Downside deviation is computed as the square root of the mean of squared
    negative returns (i.e. returns below zero), annualized by multiplying by
    sqrt(252). Returns below the threshold are used directly (not demeaned),
    so the measure reflects losses relative to zero, not relative to the mean
    of negative returns.

    Args:
        close_prices: Daily close prices series.
        risk_free_rate: Annualized risk-free rate (e.g. 0.10 for 10%).

    Returns:
        Sortino ratio as a float, or NaN if downside deviation is zero or unavailable.
    """
    returns = close_prices.pct_change().dropna()
    neg = returns[returns < 0]
    if len(neg) == 0:
        return np.nan
    dv = float(np.sqrt((neg ** 2).mean() * 252))
    return (calculate_annualized_return(close_prices) - risk_free_rate) / dv if dv > 0 else np.nan

def calculate_calmar_ratio(close_prices: pd.Series) -> float:
    """
    Computes the Calmar ratio (annualized return divided by absolute max drawdown).

    Args:
        close_prices: Daily close prices series.

    Returns:
        Calmar ratio as a float, or NaN if max drawdown is zero.
    """
    mdd = calculate_max_drawdown(close_prices)
    return -calculate_annualized_return(close_prices) / mdd if mdd < 0 else np.nan

def calculate_tracking_error(series_a: pd.Series, series_b: pd.Series, trading_days: int = 252) -> float:
    """
    Calculates the annualized tracking error between two price series.

    Args:
        series_a (pd.Series): The first price series.
        series_b (pd.Series): The second price series.
        trading_days (int): Number of trading days used for annualization. Defaults to 252.

    Returns:
        float: The annualized tracking error.
    """
    aligned_data = pd.concat([series_a, series_b], axis=1, join='inner')
    returns = aligned_data.dropna().pct_change().dropna()
    difference = returns.iloc[:, 0] - returns.iloc[:, 1]
    return np.sqrt(trading_days) * difference.std()