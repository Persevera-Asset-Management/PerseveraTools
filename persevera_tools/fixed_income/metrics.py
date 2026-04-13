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

    y = ytm / coupon_frequency
    c = coupon_rate / coupon_frequency
    n = max(1, round(years_to_maturity * coupon_frequency))

    periods = np.arange(1, n + 1)
    cash_flows = np.full(n, c)
    cash_flows[-1] += 1.0  # face value returned at maturity

    pv_flows = cash_flows / (1.0 + y) ** periods
    times = periods / coupon_frequency

    total_pv = pv_flows.sum()
    return float((times * pv_flows).sum() / total_pv) if total_pv > 0 else 0.0


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


def calculate_duration(
    code: str,
    settlement_date: Optional[Union[str, datetime, pd.Timestamp]] = None,
    ytm: Optional[float] = None,
    coupon_rate: Optional[float] = None,
    coupon_frequency: int = 1,
    maturity_date: Optional[Union[str, datetime, pd.Timestamp]] = None,
    indice: Optional[str] = None,
    use_anbima: bool = True,
) -> dict:
    """Calculate Macaulay Duration for a single fixed income asset.

    Attempts first to use the duration published by ANBIMA in the database.
    If unavailable (e.g. CRI, CRA, CDB), falls back to an analytical
    calculation using the yield to maturity and maturity date from the database.

    The analytical fallback assumes a bullet structure (no amortization) with
    periodic coupons. When coupon_rate is not provided, a par-bond approximation
    is used (coupon_rate = ytm), which is accurate for bonds trading near par.
    For zero-coupon bonds, pass coupon_rate=0.0; duration equals years_to_maturity.

    Day count conventions:
    - DI-linked bonds: DU/252 (ANBIMA holiday calendar from the database).
    - IPCA-linked and pre-fixed bonds: ACT/365.

    Args:
        code: Bond code (e.g., 'ABCD11', 'CRI-XYZ123').
        settlement_date: Calculation date. Defaults to the most recent available date.
        ytm: Annual yield to maturity as a decimal (e.g., 0.12 for 12%). When
             provided, skips the database lookup for yield. Required for bonds
             without historical series in the database (e.g. CDBs) or for
             prospective calculations using a future settlement_date.
        coupon_rate: Annual coupon rate as a decimal (e.g., 0.12 for 12%).
                     Use 0.0 for zero-coupon bonds. Overrides the par-bond
                     approximation when provided.
        coupon_frequency: Number of coupon payments per year (default: 1, annual).
        maturity_date: Bond maturity date. When provided, skips the database
                       lookup in the emissions table. Required for bonds not
                       registered in the emissions table (e.g. CDBs).
        indice: Index type used to select the day count convention: 'DI' uses
                DU/252; any other value uses ACT/365. When provided, skips the
                database lookup for this field. Defaults to 'DI' if omitted and
                maturity_date is supplied directly.
        use_anbima: If True, returns the ANBIMA-published duration when available.
    Returns:
        dict with keys:
            - 'macaulay_duration': Macaulay Duration in years.
            - 'years_to_maturity': Remaining time to maturity in years (None if ANBIMA source).
            - 'ytm': Yield to maturity used (decimal).
            - 'coupon_rate': Coupon rate used (decimal, None if ANBIMA source).
            - 'source': 'anbima' or 'calculated'.
            - 'settlement_date': The settlement date used for the calculation.
    Raises:
        ValueError: If the bond code is not found or required data is missing.
    """
    if settlement_date is None:
        settlement_dt = pd.Timestamp.today().normalize()
    elif isinstance(settlement_date, str):
        settlement_dt = pd.Timestamp(settlement_date)
    elif isinstance(settlement_date, (datetime, pd.Timestamp)):
        settlement_dt = pd.Timestamp(settlement_date)
    else:
        raise ValueError("settlement_date must be a string, datetime, or pd.Timestamp")

    end_date_str = settlement_dt.strftime('%Y-%m-%d')

    if use_anbima and ytm is None:
        try:
            anbima_dur = get_series(
                code=code, source='anbima', field='duration', end_date=end_date_str
            )
            if isinstance(anbima_dur, pd.Series):
                anbima_dur = anbima_dur.dropna()
            if not anbima_dur.empty:
                latest_date = anbima_dur.index[-1]
                latest_val = float(anbima_dur.iloc[-1])
                ytm_val = None
                try:
                    ytm_series = get_series(
                        code=code, source='anbima', field='yield_to_maturity', end_date=end_date_str
                    )
                    if not ytm_series.empty:
                        ytm_val = float(ytm_series.dropna().iloc[-1]) / 100.0
                except (ValueError, IndexError):
                    pass
                return {
                    'macaulay_duration': latest_val,
                    'years_to_maturity': None,
                    'ytm': ytm_val,
                    'coupon_rate': None,
                    'source': 'anbima',
                    'settlement_date': latest_date,
                }
        except (ValueError, IndexError, KeyError):
            pass

    if ytm is not None:
        actual_settlement = settlement_dt
        ytm_decimal = ytm
    else:
        ytm_series = get_series(
            code=code, source='anbima', field='yield_to_maturity', end_date=end_date_str
        )
        if isinstance(ytm_series, pd.Series):
            ytm_series = ytm_series.dropna()
        if ytm_series.empty:
            raise ValueError(
                f"No yield_to_maturity data found for code '{code}'. "
                "Provide ytm= explicitly for bonds without historical series or future dates."
            )
        ytm_decimal = float(ytm_series.iloc[-1]) / 100.0
        actual_settlement = ytm_series.index[-1]

    if maturity_date is not None:
        maturity_date = pd.Timestamp(maturity_date)
        if indice is None:
            indice = 'DI'
    else:
        all_emissions = get_emissions(
            selected_fields=['code', 'data_vencimento', 'indice', 'percentual_multiplicador_rentabilidade']
        )
        bond_emission = all_emissions[all_emissions['code'] == code]
        if bond_emission.empty:
            raise ValueError(
                f"No emission data found for code '{code}'. "
                "Provide maturity_date= explicitly for bonds not in the emissions table."
            )
        maturity_date = pd.Timestamp(bond_emission['data_vencimento'].iloc[0])
        indice = bond_emission['indice'].iloc[0]

    days_to_maturity = (maturity_date - actual_settlement).days
    if days_to_maturity <= 0:
        effective_coupon = coupon_rate if coupon_rate is not None else ytm_decimal
        return {
            'macaulay_duration': 0.0,
            'years_to_maturity': 0.0,
            'ytm': ytm_decimal,
            'coupon_rate': effective_coupon,
            'source': 'calculated',
            'settlement_date': actual_settlement,
        }

    if indice == 'DI':
        holidays = {h.date() for h in get_holidays()}
        all_days = pd.date_range(actual_settlement + pd.Timedelta(days=1), maturity_date, freq='D')
        bdays = sum(1 for d in all_days if d.weekday() < 5 and d.date() not in holidays)
        years_to_maturity = bdays / 252.0
    else:
        years_to_maturity = days_to_maturity / 365.0

    effective_coupon = coupon_rate if coupon_rate is not None else ytm_decimal
    macaulay_dur = _macaulay_duration(ytm_decimal, effective_coupon, years_to_maturity, coupon_frequency)

    return {
        'macaulay_duration': macaulay_dur,
        'years_to_maturity': years_to_maturity,
        'ytm': ytm_decimal,
        'coupon_rate': effective_coupon,
        'source': 'calculated',
        'settlement_date': actual_settlement,
    }
