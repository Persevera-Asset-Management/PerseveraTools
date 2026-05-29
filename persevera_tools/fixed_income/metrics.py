from datetime import datetime
from typing import Optional, Union, List
import pandas as pd
import numpy as np

from persevera_tools.fixed_income.data import get_emissions, get_series
from persevera_tools.utils.dates import get_holidays


def _macaulay_duration(
    ytm: float,
    coupon_rate: float,
    years_to_maturity: float,
    coupon_frequency: int = 1,
) -> float:
    """Calculate Macaulay Duration via discounted cash flow.

    For zero-coupon bonds (coupon_rate=0), returns years_to_maturity directly,
    since the single cash flow at maturity makes D_mac = T by definition.

    Cash-flow times are measured backwards from maturity, so the final payment
    lands exactly on the (possibly fractional) maturity date and the remaining
    coupons are spaced 1/coupon_frequency apart. This preserves the fractional
    time to maturity instead of rounding it to whole coupon periods.

    Args:
        ytm: Annual yield to maturity as a decimal (e.g., 0.12 for 12%).
        coupon_rate: Annual coupon rate as a decimal (0.0 for zero-coupon bonds).
        years_to_maturity: Remaining time to maturity in years.
        coupon_frequency: Number of coupon payments per year (default: 1).
    Returns:
        Macaulay Duration in years.
    """
    if coupon_rate == 0.0:
        return years_to_maturity
    if years_to_maturity <= 0:
        return 0.0

    f = coupon_frequency
    y = ytm / f
    c = coupon_rate / f

    # Number of cash flows: one per coupon date in (0, T], measured back from T.
    n = max(1, int(np.ceil(years_to_maturity * f - 1e-9)))
    times = years_to_maturity - np.arange(n) / f  # descending; all in (0, T]
    cash_flows = np.full(n, c)
    cash_flows[0] += 1.0  # face value paid at maturity (times[0] == T)

    pv_flows = cash_flows / (1.0 + y) ** (f * times)
    total_pv = pv_flows.sum()
    return float((times * pv_flows).sum() / total_pv) if total_pv > 0 else 0.0


def _business_days_252(
    settlement: pd.Timestamp,
    maturity: pd.Timestamp,
    holidays: np.ndarray,
) -> int:
    """Count business days in (settlement, maturity] excluding ANBIMA holidays.

    Vectorized replacement for a day-by-day loop, matching the original
    convention (settlement excluded, maturity included).

    Args:
        settlement: Settlement/calculation date.
        maturity: Bond maturity date.
        holidays: Array of holidays as numpy datetime64[D].
    Returns:
        Number of business days between settlement (exclusive) and maturity (inclusive).
    """
    begin = np.datetime64((pd.Timestamp(settlement) + pd.Timedelta(days=1)).date(), 'D')
    end = np.datetime64((pd.Timestamp(maturity) + pd.Timedelta(days=1)).date(), 'D')
    if end <= begin:
        return 0
    return int(np.busday_count(begin, end, holidays=holidays))


def calculate_spread(
    index_code: str,
    start_date: Optional[Union[str, datetime, pd.Timestamp]] = None,
    end_date: Optional[Union[str, datetime, pd.Timestamp]] = None,
    field: Union[str, List[str]] = 'yield_to_maturity',
    calculate_distribution: bool = False,
    deb_incent_lei_12431: Optional[bool] = None) -> pd.DataFrame:
    """Calculate the spread for a given index code.

    Args:
        index_code: Single index code. Supported values: 'DI', 'IPCA'.
        start_date: Optional start date filter as string 'YYYY-MM-DD', datetime, or pandas Timestamp.
        end_date: Optional end date filter as string 'YYYY-MM-DD', datetime, or pandas Timestamp.
        field: Field or list of fields to retrieve (default: 'yield_to_maturity').
        calculate_distribution: Whether to calculate the distribution of the spread.
        deb_incent_lei_12431: Whether to filter emissions under Lei 12431 (default: None).
    Returns:
        DataFrame with spread data, indexed by 'date'.
    Raises:
        ValueError: If index_code is not 'DI' or 'IPCA'.
    """
    emissions = get_emissions(index_code=index_code, deb_incent_lei_12431=deb_incent_lei_12431)

    if index_code == 'DI':
        codes = emissions[emissions['percentual_multiplicador_rentabilidade'] == 100]['code'].tolist()
        series = get_series(code=codes, source='anbima', category='credito_privado_di', start_date=start_date, end_date=end_date, field=field)
        if isinstance(series.columns, pd.MultiIndex):
            selected_field = field[0] if isinstance(field, list) and len(field) > 0 else (field if isinstance(field, str) else 'yield_to_maturity')
            if 'field' in series.columns.names:
                series = series.xs(selected_field, axis=1, level='field')
            if 'source' in series.columns.names:
                series = series.xs('anbima', axis=1, level='source')
        series = series.interpolate(limit=5)
    elif index_code == 'IPCA':
        codes = emissions['code'].tolist()
        series_ipca = get_series(code=codes, source='anbima', category='credito_privado_ipca', start_date=start_date, end_date=end_date, field=field)
        series_ipca = series_ipca.replace(0., np.nan)
        series_ipca_interpolated = series_ipca.pivot_table(index='date', columns='code', values='value').interpolate(limit=5).stack().reset_index()
        series_ipca_interpolated = pd.merge(series_ipca_interpolated, series_ipca[['code', 'reference']].drop_duplicates(), on=['code'], how='left').dropna().drop_duplicates()
        series_ipca_interpolated.columns = ['date', 'code', 'value', 'reference']

        series_titulos_publicos = get_series(code='NTN-B', category='titulos_publicos', start_date=start_date, end_date=end_date, field=field)
        if isinstance(series_titulos_publicos.index, pd.MultiIndex):
            series_titulos_publicos = series_titulos_publicos.reset_index()
        elif getattr(series_titulos_publicos.index, "name", None) in ['date', 'maturity']:
            series_titulos_publicos = series_titulos_publicos.reset_index()
        if 'source' in series_titulos_publicos.columns:
            unique_sources = series_titulos_publicos['source'].unique().tolist()
            if 'anbima' in unique_sources:
                series_titulos_publicos = series_titulos_publicos[series_titulos_publicos['source'] == 'anbima']
            series_titulos_publicos = series_titulos_publicos.drop(columns=['source'])
        series_merged = pd.merge(series_ipca_interpolated, series_titulos_publicos, left_on=['date', 'reference'], right_on=['date', 'maturity'], how='inner')
        series_merged = series_merged.drop(columns=['code_y', 'maturity', 'reference'])
        series_merged.columns = ['date', 'code', 'yield_to_maturity', 'ytm_ntnb']
        series_merged['spread'] = series_merged['yield_to_maturity'] - series_merged['ytm_ntnb']
        series = series_merged.pivot_table(index='date', columns='code', values='spread')
        series = series.interpolate(limit=5)
    else:
        raise ValueError("Invalid index code")

    emissions = emissions[emissions['code'].isin(series.columns)]

    volume_map = emissions.set_index('code')['volume_emissao']
    series = series.replace(0., np.nan)
    volume_df = series.where(series.isna(), 1) * volume_map
    weight_df = volume_df.div(volume_df.sum(axis=1), axis=0)

    spread = pd.DataFrame(index=series.index)
    spread['median'] = series.median(axis=1)
    spread['mean'] = series.mean(axis=1)
    spread['weighted_mean'] = (series * weight_df).sum(axis=1)

    if calculate_distribution:
        spread['count_above_mean'] = (series.T > spread['mean'].values).T.sum(axis=1)
        spread['count_under_mean'] = (series.T <= spread['mean'].values).T.sum(axis=1)
        spread['volume_above_mean'] = ((series.T > spread['mean'].values).T * volume_df).sum(axis=1)
        spread['volume_under_mean'] = ((series.T <= spread['mean'].values).T * volume_df).sum(axis=1)

        spread['count_yield_under_neg50bp'] = ((series.T < -0.50)).T.sum(axis=1)
        spread['count_yield_neg50_0bp'] = ((series.T >= -0.50) & (series.T < 0.)).T.sum(axis=1)
        spread['count_yield_0_50bp'] = ((series.T >= 0.) & (series.T < 0.50)).T.sum(axis=1)
        spread['count_yield_50_75bp'] = ((series.T >= 0.50) & (series.T < 0.75)).T.sum(axis=1)
        spread['count_yield_75_100bp'] = ((series.T >= 0.75) & (series.T < 1.00)).T.sum(axis=1)
        spread['count_yield_100_150bp'] = ((series.T >= 1.00) & (series.T < 1.50)).T.sum(axis=1)
        spread['count_yield_150_250bp'] = ((series.T >= 1.50) & (series.T < 2.50)).T.sum(axis=1)
        spread['count_yield_above_250bp'] = (series.T >= 2.50).T.sum(axis=1)

    return spread


def _normalize_codes(code) -> "tuple[list, bool]":
    """Normalize the ``code`` argument into a de-duplicated list of strings.

    Returns the list of codes and a flag indicating whether the input was a
    single scalar string (so the public function can return a dict instead of
    a DataFrame).
    """
    if isinstance(code, str):
        codes, single = [code], True
    elif isinstance(code, pd.Series):
        codes, single = code.dropna().astype(str).tolist(), False
    elif isinstance(code, (pd.Index, np.ndarray)):
        codes, single = [str(c) for c in code.tolist()], False
    elif isinstance(code, (list, tuple, set)):
        codes, single = [str(c) for c in code], False
    else:
        raise ValueError("code must be a string, list, tuple, set, pandas Series, or Index")

    seen = set()
    codes = [c for c in codes if c and not (c in seen or seen.add(c))]
    if not codes:
        raise ValueError("No valid codes provided")
    return codes, single


def _fetch_anbima_field(codes: list, field: str, end_date_str: str) -> dict:
    """Fetch a single ANBIMA field for one or more codes, batching the query.

    Returns a dict mapping code -> non-empty pandas Series (NaNs dropped).
    Codes without data are simply absent from the result. Never raises.
    """
    out: dict = {}
    try:
        if len(codes) == 1:
            series = get_series(code=codes[0], source='anbima', field=field, end_date=end_date_str)
            if isinstance(series, pd.DataFrame):
                series = series.iloc[:, 0]
            series = series.dropna()
            if not series.empty:
                out[codes[0]] = series
            return out

        raw = get_series(code=codes, source='all', field=field, end_date=end_date_str)
        if isinstance(raw, pd.Series):
            raw = raw.dropna()
            if not raw.empty:
                out[codes[0]] = raw
            return out

        cols = raw.columns
        if isinstance(cols, pd.MultiIndex) and 'source' in cols.names:
            if 'anbima' in cols.get_level_values('source'):
                raw = raw.xs('anbima', axis=1, level='source')
            else:
                return out
        for c in raw.columns:
            series = raw[c].dropna()
            if not series.empty:
                out[str(c)] = series
    except (ValueError, IndexError, KeyError):
        pass
    return out


def _broadcast_param(value, codes: list, name: str) -> dict:
    """Broadcast a per-asset parameter into a {code: value} mapping.

    Accepted forms:
    - None -> every code maps to None.
    - dict / pandas Series indexed by code -> looked up per code (missing -> None).
    - list / tuple / ndarray -> aligned positionally to ``codes`` (length must match).
    - scalar (str, number, datetime, Timestamp, ...) -> applied to every code.
    """
    if value is None:
        return {c: None for c in codes}
    if isinstance(value, dict):
        return {c: value.get(c) for c in codes}
    if isinstance(value, pd.Series):
        lookup = value.to_dict()
        return {c: lookup.get(c) for c in codes}
    if isinstance(value, (list, tuple, np.ndarray)):
        seq = list(value)
        if len(seq) != len(codes):
            raise ValueError(
                f"Length of '{name}' ({len(seq)}) must match the number of codes ({len(codes)}). "
                "Use a dict or pandas Series keyed by code to avoid positional alignment."
            )
        return dict(zip(codes, seq))
    return {c: value for c in codes}


def calculate_duration(
    code: Union[str, List[str], pd.Series, pd.Index],
    maturity_date: Optional[Union[str, datetime, pd.Timestamp, list, tuple, dict, pd.Series]] = None,
    settlement_date: Optional[Union[str, datetime, pd.Timestamp]] = None,
    ytm: Optional[Union[float, list, tuple, dict, pd.Series]] = None,
    coupon_rate: Optional[Union[float, list, tuple, dict, pd.Series]] = None,
    coupon_frequency: int = 1,
    indice: Optional[Union[str, list, tuple, dict, pd.Series]] = None,
    use_anbima: bool = True,
) -> Union[dict, pd.DataFrame]:
    """Calculate Macaulay Duration for one or more fixed income assets.

    Accepts a single code (str) or a collection of codes (list, tuple, set,
    pandas Series/Index, e.g. a DataFrame column). For a single code a dict is
    returned; for a collection a DataFrame indexed by code is returned.

    For each asset it attempts first to use the duration published by ANBIMA in
    the database. If unavailable (e.g. CRI, CRA, CDB), it falls back to an
    analytical calculation using the yield to maturity (from the database, or
    provided via ``ytm``) and the ``maturity_date`` supplied by the caller.
    The maturity is never looked up in the database.

    The analytical fallback assumes a bullet structure (no amortization) with
    periodic coupons. When coupon_rate is not provided, a par-bond approximation
    is used (coupon_rate = ytm), which is accurate for bonds trading near par.
    For zero-coupon bonds, pass coupon_rate=0.0; duration equals years_to_maturity.

    Day count conventions:
    - DI-linked bonds: DU/252 (ANBIMA holiday calendar from the database).
    - IPCA-linked and pre-fixed bonds: ACT/365.

    Per-asset parameters: ``maturity_date``, ``indice``, ``ytm`` and
    ``coupon_rate`` accept either a single scalar (applied to every code), a
    list/tuple aligned positionally to the codes, or a dict / pandas Series
    keyed by code (recommended for collections).

    Performance: holidays are fetched once and reused across all codes, the
    yield/duration series are fetched in a single batched query, and business
    days use ``numpy.busday_count`` instead of a day-by-day loop. No emissions
    table lookup is performed.

    Args:
        code: Single bond code or a collection of codes. Supports str, list,
              tuple, set, pandas Series (e.g. a DataFrame column), or Index.
        maturity_date: Bond maturity date(s). Required whenever the analytical
                       fallback is used (i.e. when an ANBIMA duration is not
                       available or ``ytm`` is provided). Scalar, list/tuple
                       aligned to codes, or dict/Series keyed by code.
        settlement_date: Calculation date. Defaults to the most recent available date.
        ytm: Annual yield to maturity as a decimal (e.g., 0.12 for 12%). When
             provided for a code, skips the database lookup for yield (and the
             ANBIMA duration lookup) for that code. Scalar, list/tuple, or
             dict/Series keyed by code. Required for assets without historical
             series or for prospective/future settlement dates, EXCEPT for
             zero-coupon/bullet bonds (coupon_rate=0.0), where the yield is not
             needed because duration equals the time to maturity.
        coupon_rate: Annual coupon rate as a decimal (e.g., 0.12 for 12%).
                     Use 0.0 for zero-coupon/bullet bonds such as a CDB that
                     pays principal and interest only at maturity; in that case
                     duration equals the time to maturity and no ytm is required.
                     Overrides the par-bond approximation when provided. Scalar
                     or per-code.
        coupon_frequency: Number of coupon payments per year (default: 1, annual).
        indice: Index type used to select the day count convention: 'DI' uses
                DU/252; any other value uses ACT/365. Scalar or per-code.
                Defaults to 'DI' when omitted.
        use_anbima: If True, returns the ANBIMA-published duration when available.
    Returns:
        For a single code, a dict with keys:
            - 'macaulay_duration': Macaulay Duration in years.
            - 'years_to_maturity': Remaining time to maturity in years (None if ANBIMA source).
            - 'ytm': Yield to maturity used (decimal).
            - 'coupon_rate': Coupon rate used (decimal, None if ANBIMA source).
            - 'source': 'anbima', 'calculated', or 'no_data'.
            - 'settlement_date': The settlement date used for the calculation.
        For a collection of codes, a DataFrame with those keys as columns,
        indexed by code. Codes with missing data get NaN values and source
        'no_data' (instead of raising).
    Raises:
        ValueError: For a single code, if required data (yield or maturity) is
                    missing; or if a per-code list length does not match codes.
    """
    codes, single = _normalize_codes(code)

    if settlement_date is None:
        settlement_dt = pd.Timestamp.today().normalize()
    elif isinstance(settlement_date, (str, datetime, pd.Timestamp)):
        settlement_dt = pd.Timestamp(settlement_date)
    else:
        raise ValueError("settlement_date must be a string, datetime, or pd.Timestamp")

    end_date_str = settlement_dt.strftime('%Y-%m-%d')

    # Per-asset parameters (scalar / positional list / dict / Series keyed by code).
    maturity_map = _broadcast_param(maturity_date, codes, 'maturity_date')
    indice_map = _broadcast_param(indice, codes, 'indice')
    coupon_map = _broadcast_param(coupon_rate, codes, 'coupon_rate')
    ytm_user_map = _broadcast_param(ytm, codes, 'ytm')

    # --- Batched database reads (only for codes without a user-provided ytm) ---
    codes_db = [c for c in codes if ytm_user_map[c] is None]
    dur_map: dict = {}
    ytm_map: dict = {}
    if codes_db:
        ytm_map = _fetch_anbima_field(codes_db, 'yield_to_maturity', end_date_str)
        if use_anbima:
            dur_map = _fetch_anbima_field(codes_db, 'duration', end_date_str)

    holiday_holder: dict = {}

    def _holidays_arr() -> np.ndarray:
        if 'arr' not in holiday_holder:
            holiday_holder['arr'] = np.array(
                [np.datetime64(pd.Timestamp(h).date(), 'D') for h in get_holidays()],
                dtype='datetime64[D]',
            )
        return holiday_holder['arr']

    def _no_data_row(ytm_decimal=None, settlement=None) -> dict:
        return {
            'macaulay_duration': np.nan,
            'years_to_maturity': np.nan,
            'ytm': ytm_decimal,
            'coupon_rate': np.nan,
            'source': 'no_data',
            'settlement_date': settlement,
        }

    def _compute(bond_code: str) -> dict:
        user_ytm = ytm_user_map[bond_code]
        coupon_value = coupon_map[bond_code]
        # A zero-coupon / bullet bond (e.g. a CDB paying everything at maturity)
        # has a single cash flow, so its Macaulay duration equals the time to
        # maturity and the yield is not needed.
        is_zero_coupon = coupon_value == 0.0

        # 1. ANBIMA-published duration (skipped when the caller supplies ytm).
        if use_anbima and user_ytm is None and bond_code in dur_map:
            dur_series = dur_map[bond_code]
            ytm_val = None
            if bond_code in ytm_map:
                ytm_val = float(ytm_map[bond_code].iloc[-1]) / 100.0
            return {
                'macaulay_duration': float(dur_series.iloc[-1]),
                'years_to_maturity': None,
                'ytm': ytm_val,
                'coupon_rate': None,
                'source': 'anbima',
                'settlement_date': dur_series.index[-1],
            }

        # 2. Resolve yield to maturity and the effective settlement date.
        if user_ytm is not None:
            ytm_decimal = user_ytm
            actual_settlement = settlement_dt
        elif bond_code in ytm_map:
            ytm_series = ytm_map[bond_code]
            ytm_decimal = float(ytm_series.iloc[-1]) / 100.0
            actual_settlement = ytm_series.index[-1]
        elif is_zero_coupon:
            # No yield available, but none is needed for a single-cash-flow bond.
            ytm_decimal = None
            actual_settlement = settlement_dt
        elif single:
            raise ValueError(
                f"No yield_to_maturity data found for code '{bond_code}'. "
                "Provide ytm= explicitly (or coupon_rate=0.0 for a zero-coupon/bullet "
                "bond such as a CDB) for assets without historical series or future dates."
            )
        else:
            return _no_data_row()

        # 3. Resolve maturity date (caller-supplied only) and day count convention.
        bond_maturity = maturity_map[bond_code]
        if bond_maturity is None:
            if single:
                raise ValueError(
                    f"No maturity_date provided for code '{bond_code}'. "
                    "Pass maturity_date= (scalar, list, dict, or Series keyed by code)."
                )
            return _no_data_row(ytm_decimal, actual_settlement)
        bond_maturity = pd.Timestamp(bond_maturity)
        bond_indice = indice_map[bond_code] if indice_map[bond_code] is not None else 'DI'

        effective_coupon = coupon_value if coupon_value is not None else ytm_decimal

        days_to_maturity = (bond_maturity - pd.Timestamp(actual_settlement)).days
        if days_to_maturity <= 0:
            return {
                'macaulay_duration': 0.0,
                'years_to_maturity': 0.0,
                'ytm': ytm_decimal,
                'coupon_rate': effective_coupon,
                'source': 'calculated',
                'settlement_date': actual_settlement,
            }

        if bond_indice == 'DI':
            bdays = _business_days_252(actual_settlement, bond_maturity, _holidays_arr())
            years_to_maturity = bdays / 252.0
        else:
            years_to_maturity = days_to_maturity / 365.0

        macaulay_dur = _macaulay_duration(ytm_decimal, effective_coupon, years_to_maturity, coupon_frequency)

        return {
            'macaulay_duration': macaulay_dur,
            'years_to_maturity': years_to_maturity,
            'ytm': ytm_decimal,
            'coupon_rate': effective_coupon,
            'source': 'calculated',
            'settlement_date': actual_settlement,
        }

    results = {bond_code: _compute(bond_code) for bond_code in codes}

    if single:
        return results[codes[0]]

    columns = ['macaulay_duration', 'years_to_maturity', 'ytm', 'coupon_rate', 'source', 'settlement_date']
    df = pd.DataFrame.from_dict(results, orient='index')[columns]
    df.index.name = 'code'
    return df
