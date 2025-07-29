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

def calculate_tracking_error(series_a: pd.Series, series_b: pd.Series, window: int = 252) -> float:
    """
    Calculates the tracking error between two series.

    Args:
        series_a (pd.Series): The first series.
        series_b (pd.Series): The second series.
        window (int): The window to calculate the tracking error over. Defaults to 252.

    Returns:
        float: The tracking error.
    """
    # Align the series by index and remove any rows with missing values
    aligned_data = pd.concat([series_a, series_b], axis=1, join='inner')
    
    # Calculate returns on the aligned data after removing missing values
    returns = aligned_data.dropna().pct_change().dropna()
    
    # Calculate the difference in returns
    difference = returns.iloc[:, 0] - returns.iloc[:, 1]
    
    # Calculate the annualized standard deviation of the difference (Tracking Error)
    return np.sqrt(window) * difference.std()