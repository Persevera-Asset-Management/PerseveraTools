"""
Pipeline do sub-índice IHFA Long & Short.

Reconstrói a carteira trimestral do IHFA filtrando fundos Long & Short,
renormaliza pesos, calcula a performance ponderada pelas cotas CVM e
opcionalmente faz upsert em ``indicadores``.

Código persistido: ``persevera_anbima_ihfa_long_short`` (field ``close``).
"""

from __future__ import annotations

import re
from typing import Optional, Sequence

import numpy as np
import pandas as pd

from ..db.operations import to_sql
from ..utils.logging import get_logger
from ..data.funds import get_funds_data
from ..data.providers.anbima_feed import (
    AnbimaFeedNotFoundError,
    AnbimaFeedProvider,
    AnbimaFundosProvider,
)

logger = get_logger(__name__)

# --------------------------------------------------------------------------- #
# Constantes
# --------------------------------------------------------------------------- #

INDEX_CODE = "persevera_anbima_ihfa_long_short"
INDEX_FIELD = "close"
INDEX_BASE = 100.0

QUARTER_MONTHS = (1, 4, 7, 10)

# v1: histórico até jul/2024; v2 (RCVM 175) a partir de out/2024
V1_ENDPOINT = "anbima_feed_indices_mais_carteira_teorica_ihfa"
V1_START = (2014, 1)
V1_END = (2024, 7)

V2_ENDPOINT = "anbima_feed_indices_mais_carteira_teorica_ihfa_v2"
V2_START = (2024, 10)

_LS_PATTERN = "long and short"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def format_cnpj(cnpj: object) -> str:
    """Normaliza CNPJ para o formato XX.XXX.XXX/XXXX-XX usado em ``fundos_cvm``."""
    digits = re.sub(r"\D", "", str(cnpj))
    if len(digits) == 14:
        return (
            f"{digits[:2]}.{digits[2:5]}.{digits[5:8]}/"
            f"{digits[8:12]}-{digits[12:]}"
        )
    return str(cnpj)


def _explode_nested(df: pd.DataFrame) -> pd.DataFrame:
    """Explode ``fundos`` (v1) ou ``componentes`` (v2) em linhas flat."""
    nested_col = next((c for c in ("fundos", "componentes") if c in df.columns), None)
    if nested_col is None:
        return df
    ctx_cols = [c for c in df.columns if c != nested_col]
    rows: list[dict] = []
    for _, row in df.iterrows():
        ctx = row[ctx_cols].to_dict()
        for item in (row.get(nested_col) or []):
            if isinstance(item, dict):
                rows.append({**ctx, **item})
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _fetch_quarters(
    feed: AnbimaFeedProvider,
    endpoint: str,
    start: tuple[int, int],
    end: tuple[int, int],
) -> pd.DataFrame:
    """Baixa todas as carteiras trimestrais no intervalo ``start``–``end``."""
    today = pd.Timestamp.today()
    sy, sm = start
    ey, em = end
    frames: list[pd.DataFrame] = []

    for year in range(sy, ey + 1):
        for month in QUARTER_MONTHS:
            if (year, month) < (sy, sm) or (year, month) > (ey, em):
                continue
            if (year, month) > (today.year, today.month):
                break
            try:
                df = feed.get_data(endpoint, mes=month, ano=year)
                if not df.empty:
                    df["_mes"], df["_ano"] = month, year
                    frames.append(df)
                    logger.info("%s %04d-%02d: %d períodos", endpoint, year, month, len(df))
            except (AnbimaFeedNotFoundError, Exception) as exc:
                logger.debug("Sem dados %s %04d-%02d: %s", endpoint, year, month, exc)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# --------------------------------------------------------------------------- #
# Etapas do pipeline
# --------------------------------------------------------------------------- #

def fetch_composition(
    feed: Optional[AnbimaFeedProvider] = None,
) -> pd.DataFrame:
    """
    Retorna a composição histórica consolidada do IHFA (v1 + v2).

    Colunas principais: ``identificador``, ``cnpj``, ``nome``, ``data_inicio``,
    ``data_fim``, ``peso``, ``_versao``, ``_mes``, ``_ano``.
    """
    feed = feed or AnbimaFeedProvider()
    today = pd.Timestamp.today()

    logger.info("Baixando carteiras IHFA v1...")
    raw_v1 = _fetch_quarters(feed, V1_ENDPOINT, V1_START, V1_END)

    logger.info("Baixando carteiras IHFA v2...")
    raw_v2 = _fetch_quarters(feed, V2_ENDPOINT, V2_START, (today.year, 12))

    parts: list[pd.DataFrame] = []

    if not raw_v1.empty:
        df = _explode_nested(raw_v1).rename(columns={
            "cnpj_fundo": "cnpj",
            "nome_fundo": "nome",
            "valor_quota": "valor_cota",
            "valor_patrimonio_liquido": "pl",
        })
        df["identificador"] = df["cnpj"]
        df["_versao"] = "v1"
        parts.append(df)

    if not raw_v2.empty:
        # v2: componentes trazem cnpj_classe (não codigo_subclasse)
        df = _explode_nested(raw_v2).rename(columns={
            "cnpj_classe": "cnpj",
            "razao_social_classe": "nome",
            "valor_patrimonio_liquido": "pl",
        })
        df["identificador"] = df["cnpj"]
        df["_versao"] = "v2"
        parts.append(df)

    if not parts:
        raise RuntimeError("Nenhuma carteira IHFA retornada pela API ANBIMA.")

    df_comp = pd.concat(parts, ignore_index=True)
    for col in ("data_inicio", "data_fim"):
        df_comp[col] = pd.to_datetime(df_comp[col], errors="coerce")
    df_comp["peso"] = pd.to_numeric(df_comp["peso"], errors="coerce")
    df_comp["cnpj_fundo"] = df_comp["cnpj"]

    antes = len(df_comp)
    df_comp = (
        df_comp
        .dropna(subset=["identificador", "data_inicio", "data_fim"])
        .drop_duplicates(subset=["identificador", "data_inicio", "data_fim"])
    )
    logger.info(
        "Composicao: %d registros (%d removidos) | %d fundos | cobertura %s -> %s",
        len(df_comp),
        antes - len(df_comp),
        df_comp["identificador"].nunique(),
        df_comp["data_inicio"].min().date(),
        df_comp["data_fim"].max().date(),
    )
    return df_comp


def classify_and_filter_ls(
    df_comp: pd.DataFrame,
    fundos: Optional[AnbimaFundosProvider] = None,
) -> pd.DataFrame:
    """
    Classifica fundos via API ANBIMA e filtra Long & Short.

    Adiciona ``tipo_anbima``, ``classe_anbima`` e ``peso_ls`` (renormalizado
    para 100% dentro de cada período de vigência).
    """
    fundos = fundos or AnbimaFundosProvider()
    logger.info("Pre-carregando cache de CNPJs (FIF)...")
    fundos.pre_load_cnpj_cache(tipo_fundo="FIF")

    ids = df_comp["identificador"].dropna().unique()
    total = len(ids)
    logger.info("Classificando %d fundos unicos...", total)

    registros: list[dict] = []
    for i, ident in enumerate(ids, 1):
        try:
            d = fundos.get_fundo_detalhes(ident)
            cls = (d.get("classes") or [{}])[0]
            registros.append({
                "identificador": ident,
                "tipo_anbima": cls.get("tipo_anbima") or d.get("tipo_anbima"),
                "classe_anbima": cls.get("classe_anbima") or d.get("classe_anbima"),
            })
        except Exception as exc:
            logger.debug("Falha ao classificar %s: %s", ident, exc)
            registros.append({
                "identificador": ident,
                "tipo_anbima": None,
                "classe_anbima": None,
            })
        if i % 50 == 0 or i == total:
            logger.info("  [%d/%d] classificados", i, total)

    df = df_comp.merge(pd.DataFrame(registros), on="identificador", how="left")

    mask = (
        df["tipo_anbima"].fillna("").str.lower().str.contains(_LS_PATTERN)
        | df["classe_anbima"].fillna("").str.lower().str.contains(_LS_PATTERN)
    )
    df_ls = df[mask].copy()
    if df_ls.empty:
        raise RuntimeError("Nenhum fundo Long & Short encontrado na composição IHFA.")

    df_ls["peso_ls"] = df_ls.groupby(["data_inicio", "data_fim"])["peso"].transform(
        lambda x: x / x.sum() * 100
    )

    logger.info(
        "L&S: %d fundos | cobertura %s -> %s | peso medio no IHFA: %.1f%%",
        df_ls["cnpj_fundo"].nunique(),
        df_ls["data_inicio"].min().date(),
        df_ls["data_fim"].max().date(),
        df_ls.groupby(["data_inicio", "data_fim"])["peso"].sum().mean(),
    )
    return df_ls


def build_index(df_ls: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """
    Constrói o sub-índice a partir dos pesos L&S e das cotas CVM.

    Returns:
        ``(indice, retorno_diario)`` — índice acumulado (base 100) e retornos.
    """
    cnpjs_raw = df_ls["cnpj_fundo"].dropna().unique().tolist()
    cnpj_map = {c: format_cnpj(c) for c in cnpjs_raw}
    dt_inicio = df_ls["data_inicio"].min().strftime("%Y-%m-%d")

    logger.info("Buscando NAV de %d fundos L&S a partir de %s...", len(cnpj_map), dt_inicio)
    df_cota = get_funds_data(
        cnpjs=list(cnpj_map.values()),
        start_date=dt_inicio,
        fields=["fund_nav"],
    )
    if df_cota.empty:
        raise RuntimeError("Nenhuma cota encontrada em fundos_cvm para os CNPJs L&S.")

    logger.info(
        "NAV: %s -> %s | %d/%d fundos com dados",
        df_cota.index.min().date(),
        df_cota.index.max().date(),
        df_cota.notna().any().sum(),
        len(cnpjs_raw),
    )

    df_ret = df_cota.pct_change(fill_method=None)

    df_pesos = (
        df_ls[["data_inicio", "data_fim", "cnpj_fundo", "peso_ls"]]
        .dropna(subset=["cnpj_fundo"])
        .drop_duplicates()
    )
    df_pesos = df_pesos.copy()
    df_pesos["cnpj_fundo"] = df_pesos["cnpj_fundo"].map(cnpj_map)

    chunks: list[pd.DataFrame] = []
    for _, row in df_pesos.iterrows():
        chunks.append(pd.DataFrame({
            "date": pd.bdate_range(row.data_inicio, row.data_fim),
            "cnpj_fundo": row.cnpj_fundo,
            "peso": row.peso_ls,
        }))

    df_w = (
        pd.concat(chunks, ignore_index=True)
        .pivot_table(index="date", columns="cnpj_fundo", values="peso", aggfunc="last")
        .fillna(0)
    )

    dates_ok = df_ret.index.intersection(df_w.index)
    funds_ok = df_ret.columns.intersection(df_w.columns)
    if funds_ok.empty:
        raise RuntimeError("Nenhum fundo em comum entre retornos e pesos.")

    logger.info("Fundos com retorno e peso: %d/%d", len(funds_ok), len(cnpjs_raw))

    w = df_w.loc[dates_ok, funds_ok]
    r = df_ret.loc[dates_ok, funds_ok]
    w_av = w.where(r.notna(), 0)
    w_norm = w_av.div(w_av.sum(axis=1).replace(0, np.nan), axis=0)

    ret = (w_norm * r).sum(axis=1, min_count=1).rename("retorno")
    indice = ((1 + ret).cumprod() * INDEX_BASE).rename(INDEX_CODE)
    return indice, ret


def index_to_long(
    indice: pd.Series,
    code: str = INDEX_CODE,
    field: str = INDEX_FIELD,
) -> pd.DataFrame:
    """Converte a série do índice para o formato long de ``indicadores``."""
    out = (
        indice.dropna()
        .rename("value")
        .reset_index()
        .rename(columns={indice.index.name or "index": "date"})
    )
    if out.columns[0] != "date":
        out = out.rename(columns={out.columns[0]: "date"})
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["code"] = code
    out["field"] = field
    out = out.dropna(subset=["date", "value"])
    return out[["date", "code", "field", "value"]]


# --------------------------------------------------------------------------- #
# Entrypoint
# --------------------------------------------------------------------------- #

def run_anbima_ihfa_ls_pipeline(
    *,
    upload: bool = True,
    table_name: str = "indicadores",
    primary_keys: Sequence[str] = ("code", "date", "field"),
    batch_size: int = 5000,
    code: str = INDEX_CODE,
    field: str = INDEX_FIELD,
) -> pd.DataFrame:
    """
    Executa o pipeline completo do sub-índice IHFA Long & Short.

    Args:
        upload: Se ``True``, faz upsert em ``table_name``.
        table_name: Tabela destino (padrão ``indicadores``).
        primary_keys: Chaves de conflito do upsert.
        batch_size: Tamanho do batch em ``to_sql``.
        code: Código do indicador (padrão ``persevera_anbima_ihfa_long_short``).
        field: Campo persistido (padrão ``close``).

    Returns:
        DataFrame long ``[date, code, field, value]`` pronto para ``indicadores``.
    """
    logger.info("=== Pipeline IHFA Long & Short ===")

    feed = AnbimaFeedProvider()
    fundos = AnbimaFundosProvider()

    df_comp = fetch_composition(feed)
    df_ls = classify_and_filter_ls(df_comp, fundos)
    indice, ret = build_index(df_ls)

    result = index_to_long(indice, code=code, field=field)
    result = result.drop_duplicates(subset=list(primary_keys), keep="last")

    if not result.empty:
        logger.info(
            "Indice %s: %s -> %s | ultimo=%.2f | %d linhas",
            code,
            result["date"].min().date(),
            result["date"].max().date(),
            result["value"].iloc[-1],
            len(result),
        )
        anual = (
            ret.resample("YE")
            .apply(lambda x: (1 + x).prod() - 1)
            .mul(100)
            .round(2)
        )
        logger.info("Retorno anual (%%):\n%s", anual.to_string())
    else:
        logger.warning("Pipeline não produziu linhas")

    if upload and not result.empty:
        logger.info("Upserting %d rows into '%s' (code=%s)", len(result), table_name, code)
        to_sql(
            result.reset_index(drop=True),
            table_name=table_name,
            primary_keys=list(primary_keys),
            update=True,
            batch_size=batch_size,
        )
    elif upload:
        logger.info("Skip upload - sem linhas")

    return result

if __name__ == "__main__":
    run_anbima_ihfa_ls_pipeline()