import numpy as np
import pandas as pd


def transform_series(series: pd.Series, tcode: int) -> pd.Series:
    """
    Applies the specified transformation to a single data series.
    Based on the FRED-MD documentation.

    tcode 1: No transformation
    tcode 2: Δx_t
    tcode 3: Δ^2x_t
    tcode 4: log(x_t)
    tcode 5: Δlog(x_t)
    tcode 6: Δ^2log(x_t)
    tcode 7: Δ(x_t/x_{t-1} - 1)
    """
    tcode = int(tcode)
    
    # Drop NaNs for transformation purposes
    series = series.dropna()

    if tcode == 1:
        return series
    elif tcode == 2:
        return series.diff()
    elif tcode == 3:
        return series.diff().diff()
    elif tcode == 4:
        return np.log(series)
    elif tcode == 5:
        return np.log(series).diff()
    elif tcode == 6:
        return np.log(series).diff().diff()
    elif tcode == 7:
        return (series / series.shift(1) - 1).diff()
    else:
        raise ValueError(f"Unknown transformation code: {tcode}")
