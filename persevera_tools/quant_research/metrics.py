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
    close_difference = close_prices.pct_change()
    sma_close_difference = close_difference.rolling(window=period).mean()
    stdev_close_difference = close_difference.rolling(window=period).std()

    sqn = (sma_close_difference * np.sqrt(period)) / stdev_close_difference

    return sqn


def get_sqn_categories(sqn: pd.Series) -> pd.Series:
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