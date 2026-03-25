from datetime import datetime
from typing import Optional, Union, List
import pandas as pd
import numpy as np

from persevera_tools.fixed_income.data import get_emissions, get_series


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
