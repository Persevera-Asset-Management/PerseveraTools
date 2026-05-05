"""
Loader Fibery → SpectrumConfig.

Pull da Calibração mais recente (ou específica por ID/data) das tabelas:
  • Inv-Rsrch-Quant/Calibração de Mercado
  • Inv-Rsrch-Quant/Classes de Ativo (catálogo)
  • Inv-Rsrch-Quant/Parâmetros de Classe (vol/maxw/maxrc)
  • Inv-Rsrch-Quant/RC Target por Bucket (RC P1/P10)
  • Inv-Rsrch-Quant/Correlação Intra-Classe (rho intra-bucket)
  • Inv-Rsrch-Quant/Correlação Macro-Bucket (rho cross-bucket)

Estratégia de falha: explícita. Se qualquer pull falhar, a calibração estiver
incompleta, ou os dados estiverem inconsistentes, levanta exception com
mensagem clara — sem fallback silencioso.

Uso:
    from persevera_tools.quant_research.hrp.loaders import load_from_fibery
    config = load_from_fibery()                    # mais recente Aprovada
    config = load_from_fibery(status="Rascunho")   # mais recente Rascunho
    config = load_calibration_by_date("2026-05-04")
    config = load_calibration_by_name("Calibração de Mercado 2026-05-04")
"""

from __future__ import annotations
from typing import Optional

import numpy as np
import pandas as pd

from persevera_tools.db.fibery import read_fibery

from ..config import (
    SpectrumConfig,
    AssetClass,
    BucketConfig,
    RCTarget,
)


# ── Constantes (nomes de databases e campos no Fibery) ────────────────────────

SPACE_NAME        = "Inv-Rsrch-Quant"
DB_CALIBRATION    = f"{SPACE_NAME}/Calibração de Mercado"
DB_ASSET_CLASS    = f"{SPACE_NAME}/Classes de Ativo"
DB_PARAMETERS     = f"{SPACE_NAME}/Parâmetros de Classe"
DB_RC_TARGET      = f"{SPACE_NAME}/RC Target por Bucket"
DB_CORR_INTRA     = f"{SPACE_NAME}/Correlação Intra-Classe"
DB_CORR_MACRO     = f"{SPACE_NAME}/Correlação Macro-Bucket"


class FiberyLoaderError(Exception):
    """Erro específico do loader Fibery (dados ausentes/inconsistentes)."""
    pass


# ── API pública ───────────────────────────────────────────────────────────────

def load_from_fibery(status: Optional[str] = "Aprovada") -> SpectrumConfig:
    """
    Carrega a Calibração mais recente em status especificado.

    Parâmetros
    ----------
    status : str ou None
        Filtro por status do workflow. Default: "Aprovada".
        Se None, pega a mais recente independente de status.

    Levanta
    -------
    FiberyLoaderError : se nenhuma calibração no status existir, ou se a
        calibração mais recente estiver incompleta.
    """
    calib_df = _read_calibrations()

    if status is not None:
        calib_df = calib_df[calib_df["status"] == status]
        if calib_df.empty:
            raise FiberyLoaderError(
                f"Nenhuma Calibração de Mercado em status '{status}' encontrada no Fibery"
            )

    # Mais recente por Data Calibração (desempate: created date se houver)
    calib_df = calib_df.sort_values("data_calibracao", ascending=False)
    calib_row = calib_df.iloc[0]

    return _build_config_from_calibration(
        calibration_name=calib_row["name"],
        calibration_date=str(calib_row["data_calibracao"]),
        calibration_status=calib_row["status"],
    )


def load_calibration_by_name(calibration_name: str) -> SpectrumConfig:
    """Carrega calibração específica por ID (UUID do Fibery)."""
    calib_df = _read_calibrations()
    match = calib_df[calib_df["name"] == calibration_name]
    if match.empty:
        raise FiberyLoaderError(f"Calibração com nome '{calibration_name}' não encontrada")
    row = match.iloc[0]
    return _build_config_from_calibration(
        calibration_name=row["name"],
        calibration_date=str(row["data_calibracao"]),
        calibration_status=row["status"],
    )


def load_calibration_by_date(date: str) -> SpectrumConfig:
    """
    Carrega calibração com Data Calibração específica.
    `date` no formato 'YYYY-MM-DD'.
    """
    calib_df = _read_calibrations()
    match = calib_df[calib_df["data_calibracao"].astype(str) == date]
    if match.empty:
        raise FiberyLoaderError(f"Calibração com data '{date}' não encontrada")
    if len(match) > 1:
        raise FiberyLoaderError(
            f"Múltiplas calibrações com data '{date}' — use load_calibration_by_name"
        )
    row = match.iloc[0]
    return _build_config_from_calibration(
        calibration_name=row["name"],
        calibration_date=str(row["data_calibracao"]),
        calibration_status=row["status"],
    )


# ── Construção da SpectrumConfig a partir da calibração ───────────────────────

def _build_config_from_calibration(
    calibration_name: str,
    calibration_date: str,
    calibration_status: str,
) -> SpectrumConfig:
    """
    Pull de todas as tabelas filhas da calibração e montagem da SpectrumConfig.
    """
    # 1. Catálogo de classes (independente de calibração)
    classes_df = _read_classes_catalog()

    # 2. Parâmetros desta calibração
    params_df = _read_parameters(calibration_name)
    if params_df.empty:
        raise FiberyLoaderError(
            f"Calibração '{calibration_name}' não tem Parâmetros de Classe associados"
        )

    # 3. RC targets desta calibração
    rc_df = _read_rc_targets(calibration_name)
    if rc_df.empty:
        raise FiberyLoaderError(
            f"Calibração '{calibration_name}' não tem RC Targets associados"
        )

    # 4. Correlações intra-classe desta calibração
    intra_df = _read_corr_intra(calibration_name)
    if intra_df.empty:
        raise FiberyLoaderError(
            f"Calibração '{calibration_name}' não tem Correlações Intra-Classe"
        )

    # 5. Correlações macro-bucket desta calibração
    macro_df = _read_corr_macro(calibration_name)
    if macro_df.empty:
        raise FiberyLoaderError(
            f"Calibração '{calibration_name}' não tem Correlações Macro-Bucket"
        )

    # ── Montar AssetClass list (mantendo ordem do catálogo)
    # JOIN catálogo + parâmetros via class_id
    merged = classes_df.merge(
        params_df, left_on="name", right_on="class_id", how="inner"
    )
    if len(merged) != len(classes_df):
        missing = set(classes_df["id"]) - set(merged["id"])
        missing_names = classes_df[classes_df["id"].isin(missing)]["name"].tolist()
        raise FiberyLoaderError(
            f"Calibração '{calibration_name}' não tem parâmetros para as classes: "
            f"{missing_names}"
        )

    # Bucketizar: ordem dos buckets = ordem em que aparecem nas classes
    # (fixa pelo Fibery via ordem do catálogo)
    bucket_order: list[str] = []
    for bk in merged["bucket"]:
        if bk not in bucket_order:
            bucket_order.append(bk)

    # Construir AssetClass list AGRUPADAS POR BUCKET (estrutural)
    assets: list[AssetClass] = []
    for bk in bucket_order:
        rows = merged[merged["bucket"] == bk]
        for _, r in rows.iterrows():
            assets.append(AssetClass(
                name=r["name"],
                bucket=r["bucket"],
                proxy=r["proxy"] or "",
                default_vol=float(r["default_vol"]),
                max_weight=float(r["max_weight"]),
                max_rc=float(r["max_rc"]) if pd.notna(r["max_rc"]) else None,
            ))

    # ── Buckets config
    # `intra_max_weight` não está no Fibery hoje; usa None (sem limite).
    # No futuro, adicionar campo no schema se necessário.
    buckets: dict[str, BucketConfig] = {
        bk: BucketConfig(
            name=bk,
            label=bk,                 # idem name por enquanto
            intra_max_weight=None,    # TODO: adicionar campo no Fibery
        )
        for bk in bucket_order
    }

    # ── RC Targets
    # Verifica que cada bucket tem exatamente um RC target
    rc_targets: dict[str, RCTarget] = {}
    for bk in bucket_order:
        rows = rc_df[rc_df["bucket"] == bk]
        if rows.empty:
            raise FiberyLoaderError(
                f"Bucket '{bk}' não tem RC Target nesta calibração"
            )
        if len(rows) > 1:
            raise FiberyLoaderError(
                f"Bucket '{bk}' tem {len(rows)} RC Targets duplicados"
            )
        r = rows.iloc[0]
        rc_targets[bk] = RCTarget(
            bucket=bk,
            rc_p1=float(r["rc_p1"]),
            rc_p10=float(r["rc_p10"]),
        )

    # ── Matriz de correlação intra-classe (uma por bucket)
    intra_corrs = _build_intra_corr_matrices(assets, bucket_order, intra_df)

    # ── Matriz de correlação macro-bucket
    macro_corr = _build_macro_corr_matrix(bucket_order, macro_df)

    # ── Montar SpectrumConfig
    config = SpectrumConfig(
        assets=assets,
        buckets=buckets,
        rc_targets=rc_targets,
        intra_corrs=intra_corrs,
        macro_corr=macro_corr,
        calibration_name=calibration_name,
        calibration_date=calibration_date,
        calibration_status=calibration_status,
    )

    # Valida antes de retornar — falha cedo se algo estiver inconsistente
    config.validate()
    return config


# ── Helpers de leitura (camada I/O) ───────────────────────────────────────────
#
# Convenção: cada função recebe o nome da calibração (quando aplicável) e retorna
# DataFrame com colunas normalizadas. Levanta FiberyLoaderError em caso de
# campos ausentes ou erros de leitura.

def _read_calibrations() -> pd.DataFrame:
    """
    Lê Calibração de Mercado. Colunas esperadas:
        id, name, data_calibracao, status
    """
    try:
        df = read_fibery(DB_CALIBRATION)
    except Exception as exc:
        raise FiberyLoaderError(
            f"Falha ao ler '{DB_CALIBRATION}' do Fibery: {exc}"
        ) from exc

    return _normalize_columns(df, {
        "id": ["fibery/id", "id", "Id"],
        "name": ["Inv-Rsrch-Quant/Name", "Name", "name"],
        "data_calibracao": ["Inv-Rsrch-Quant/Data Calibração", "Data Calibração", "data_calibracao"],
        "status": ["workflow/state", "State", "state", "Status"],
    }, table_name=DB_CALIBRATION)


def _read_classes_catalog() -> pd.DataFrame:
    """
    Lê Classes de Ativo. Colunas esperadas:
        id, name, bucket, proxy
    """
    try:
        df = read_fibery(DB_ASSET_CLASS)
    except Exception as exc:
        raise FiberyLoaderError(
            f"Falha ao ler '{DB_ASSET_CLASS}' do Fibery: {exc}"
        ) from exc

    return _normalize_columns(df, {
        "id": ["fibery/id", "id"],
        "name": ["Inv-Rsrch-Quant/Name", "Name", "name"],
        "bucket": ["Classificação Layer 1", "bucket", "Bucket"],
        "proxy": ["Proxy", "proxy"],
    }, table_name=DB_ASSET_CLASS)


def _read_parameters(calibration_name: str) -> pd.DataFrame:
    """
    Lê Parâmetros de Classe. Filtra pela calibração especificada.
    Colunas esperadas: classe_id, default_vol, max_weight, max_rc
    """
    try:
        df = read_fibery(DB_PARAMETERS)
    except Exception as exc:
        raise FiberyLoaderError(
            f"Falha ao ler '{DB_PARAMETERS}' do Fibery: {exc}"
        ) from exc

    df = _normalize_columns(df, {
        "calibration_name": ["Calibração", "calibracao_id", "Calibracao"],
        "class_id": ["Classe", "classe_id"],
        "default_vol": ["Default Vol", "default_vol"],
        "max_weight": ["Max Weight", "max_weight"],
        "max_rc": ["Max RC", "max_rc"]
    }, table_name=DB_PARAMETERS)

    # Filtra pela calibração
    return df[df["calibration_name"] == calibration_name].copy()


def _read_rc_targets(calibration_name: str) -> pd.DataFrame:
    """Lê RC Target por Bucket. Filtra pela calibração."""
    try:
        df = read_fibery(DB_RC_TARGET)
    except Exception as exc:
        raise FiberyLoaderError(
            f"Falha ao ler '{DB_RC_TARGET}' do Fibery: {exc}"
        ) from exc

    df = _normalize_columns(df, {
        "calibration_name": ["Calibração", "calibracao_id"],
        "bucket": ["Classificação Layer 1", "bucket"],
        "rc_p1": ["RC P1", "rc_p1"],
        "rc_p10": ["RC P10", "rc_p10"],
    }, table_name=DB_RC_TARGET)

    return df[df["calibration_name"] == calibration_name].copy()


def _read_corr_intra(calibration_name: str) -> pd.DataFrame:
    """Lê Correlação Intra-Classe. Filtra pela calibração."""
    try:
        df = read_fibery(DB_CORR_INTRA)
    except Exception as exc:
        raise FiberyLoaderError(
            f"Falha ao ler '{DB_CORR_INTRA}' do Fibery: {exc}"
        ) from exc

    df = _normalize_columns(df, {
        "calibration_name": ["Calibração", "calibracao_id"],
        "classe_a": ["Classe A", "classe_a"],
        "classe_b": ["Classe B", "classe_b"],
        "rho": ["Rho", "rho"],
    }, table_name=DB_CORR_INTRA)

    return df[df["calibration_name"] == calibration_name].copy()


def _read_corr_macro(calibration_name: str) -> pd.DataFrame:
    """Lê Correlação Macro-Bucket. Filtra pela calibração."""
    try:
        df = read_fibery(DB_CORR_MACRO)
    except Exception as exc:
        raise FiberyLoaderError(
            f"Falha ao ler '{DB_CORR_MACRO}' do Fibery: {exc}"
        ) from exc

    df = _normalize_columns(df, {
        "calibration_name": ["Calibração", "calibracao_id"],
        "bucket_a": ["Bucket A", "bucket_a"],
        "bucket_b": ["Bucket B", "bucket_b"],
        "rho": ["Rho", "rho"],
    }, table_name=DB_CORR_MACRO)

    return df[df["calibration_name"] == calibration_name].copy()


# ── Construção das matrizes a partir dos pares ────────────────────────────────

def _build_intra_corr_matrices(
    assets: list[AssetClass],
    bucket_order: list[str],
    intra_df: pd.DataFrame,
) -> dict[str, np.ndarray]:
    """
    Para cada bucket, monta matriz n_b × n_b a partir dos pares (i ≤ j) na
    base. Diagonal = 1, e par (j, i) é espelhado de (i, j).

    `intra_df` tem colunas: classe_a (id ou nome), classe_b, rho.
    """
    # Mapeia cada classe → índice global e bucket
    class_to_idx_in_bucket: dict[str, dict[str, int]] = {bk: {} for bk in bucket_order}
    bucket_classes: dict[str, list[str]] = {bk: [] for bk in bucket_order}
    for a in assets:
        idx = len(bucket_classes[a.bucket])
        class_to_idx_in_bucket[a.bucket][a.name] = idx
        bucket_classes[a.bucket].append(a.name)

    # Inicializa matrizes identidade
    matrices: dict[str, np.ndarray] = {
        bk: np.eye(len(bucket_classes[bk])) for bk in bucket_order
    }

    # Determina se classe_a/classe_b são IDs ou nomes (depende do read_fibery)
    # Tenta primeiro como nome (mais legível); se não bater, tenta como ID.
    name_to_bucket: dict[str, str] = {a.name: a.bucket for a in assets}

    for _, row in intra_df.iterrows():
        ca = row["classe_a"]
        cb = row["classe_b"]
        rho = float(row["rho"])

        # Resolve para nome se vier como dict (relação) ou como string
        ca_name = _resolve_relation_value(ca, assets)
        cb_name = _resolve_relation_value(cb, assets)

        if ca_name is None or cb_name is None:
            raise FiberyLoaderError(
                f"Não consegui resolver par de correlação intra: ({ca}, {cb})"
            )

        bk_a = name_to_bucket.get(ca_name)
        bk_b = name_to_bucket.get(cb_name)
        if bk_a != bk_b:
            raise FiberyLoaderError(
                f"Correlação Intra-Classe entre classes de buckets diferentes: "
                f"'{ca_name}' ({bk_a}) × '{cb_name}' ({bk_b}). "
                f"Use Correlação Macro-Bucket para pares cross-bucket."
            )

        idx_a = class_to_idx_in_bucket[bk_a][ca_name]
        idx_b = class_to_idx_in_bucket[bk_a][cb_name]
        matrices[bk_a][idx_a, idx_b] = rho
        matrices[bk_a][idx_b, idx_a] = rho

    return matrices


def _build_macro_corr_matrix(
    bucket_order: list[str],
    macro_df: pd.DataFrame,
) -> np.ndarray:
    """
    Monta matriz k × k entre buckets a partir dos pares (i ≤ j).
    """
    n = len(bucket_order)
    M = np.eye(n)
    bk_to_idx = {bk: i for i, bk in enumerate(bucket_order)}

    for _, row in macro_df.iterrows():
        ba = _resolve_bucket_value(row["bucket_a"])
        bb = _resolve_bucket_value(row["bucket_b"])
        rho = float(row["rho"])

        if ba is None or bb is None:
            raise FiberyLoaderError(
                f"Não consegui resolver par macro: ({row['bucket_a']}, {row['bucket_b']})"
            )

        if ba not in bk_to_idx or bb not in bk_to_idx:
            raise FiberyLoaderError(
                f"Bucket '{ba}' ou '{bb}' não está na lista de buckets: {bucket_order}"
            )

        ia = bk_to_idx[ba]
        ib = bk_to_idx[bb]
        M[ia, ib] = rho
        M[ib, ia] = rho

    return M


# ── Utilitários ───────────────────────────────────────────────────────────────

def _normalize_columns(
    df: pd.DataFrame,
    mapping: dict[str, list[str]],
    table_name: str,
) -> pd.DataFrame:
    """
    Normaliza colunas do DataFrame retornado por read_fibery para nomes
    canônicos esperados pelo loader. Cada chave do mapping é o nome
    canônico; o valor é uma lista de nomes possíveis no DataFrame.

    Levanta FiberyLoaderError se uma coluna obrigatória não for encontrada.
    """
    normalized = pd.DataFrame()
    missing: list[str] = []

    for canonical, candidates in mapping.items():
        found = None
        for cand in candidates:
            if cand in df.columns:
                found = cand
                break
        if found is None:
            missing.append(f"{canonical} (tentou: {candidates})")
        else:
            normalized[canonical] = df[found].values

    if missing:
        raise FiberyLoaderError(
            f"Colunas ausentes em '{table_name}':\n  " + "\n  ".join(missing) +
            f"\nColunas disponíveis: {list(df.columns)}"
        )

    return normalized


def _resolve_relation_value(value, assets: list[AssetClass]) -> Optional[str]:
    """
    Resolve um valor de campo de relação para o nome da classe.
    Pode vir como:
      • string (nome direto)
      • dict {'Name': '...', 'Id': '...'}
      • UUID (se for ID, busca na lista de assets pelo ID)
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    if isinstance(value, str):
        # Tenta como nome direto primeiro
        for a in assets:
            if a.name == value:
                return a.name
        # Não bateu como nome — pode ser ID
        return value  # devolve a string e deixa o caller validar
    if isinstance(value, dict):
        return value.get("Name") or value.get("name")
    return None


def _resolve_bucket_value(value) -> Optional[str]:
    """Resolve um valor de campo de relação de bucket para nome."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return value.get("Name") or value.get("name")
    return None
