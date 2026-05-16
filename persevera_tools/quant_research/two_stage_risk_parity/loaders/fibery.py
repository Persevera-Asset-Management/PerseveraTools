"""
Loader Fibery → SpectrumConfig.

Pull da Calibração mais recente (ou específica por ID/data) das tabelas:
  • Inv-Rsrch-Quant/Calibração de Mercado
  • Inv-Rsrch-Quant/Classes de Ativo (catálogo)
  • Inv-Rsrch-Quant/Parâmetros de Classe (vol/maxw/maxrc)
  • Inv-Rsrch-Quant/RC Target por Bucket (RC P1/P10)
  • Inv-Rsrch-Quant/Correlação Cross-Classe (rho para TODOS os pares de classes)

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
DB_CORR_CLASSES   = f"{SPACE_NAME}/Correlação Cross-Classe"


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
        sigma_min=calib_row["sigma_min"],
        sigma_max=calib_row["sigma_max"],
        n_profiles=calib_row["n_profiles"],
        min_weight_threshold=calib_row["min_weight_threshold"],
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
        sigma_min=row["sigma_min"],
        sigma_max=row["sigma_max"],
        n_profiles=row["n_profiles"],
        min_weight_threshold=row["min_weight_threshold"],
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
        sigma_min=row["sigma_min"],
        sigma_max=row["sigma_max"],
        n_profiles=row["n_profiles"],
        min_weight_threshold=row["min_weight_threshold"],
    )


# ── Construção da SpectrumConfig a partir da calibração ───────────────────────

def _build_config_from_calibration(
    calibration_name: str,
    calibration_date: str,
    calibration_status: str,
    sigma_min: float,
    sigma_max: float,
    n_profiles: int,
    min_weight_threshold: float,
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

    # 4. Correlações entre classes desta calibração (todos os pares)
    corr_df = _read_corr_classes(calibration_name)
    if corr_df.empty:
        raise FiberyLoaderError(
            f"Calibração '{calibration_name}' não tem Correlações Cross-Classe"
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
                proxy=r["proxy"] if pd.notna(r["proxy"]) else "",
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

    # ── Matriz de correlação completa (n × n, todos os pares)
    full_corr = _build_full_corr_matrix(assets, corr_df)

    # Hard validation: faixa de vol viável dado o universo
    sigma_min_feas, sigma_max_feas = _compute_feasible_vol_range(assets, full_corr)

    if sigma_min < sigma_min_feas - 1e-6:
        raise FiberyLoaderError(
            f"Calibração '{calibration_name}': Sigma Min = {sigma_min*100:.4f}% "
            f"é inviável dado o universo. Vol mínima atingível (min-variance "
            f"long-only) = {sigma_min_feas*100:.4f}%. "
            f"Ajuste Sigma Min no Fibery para >= {sigma_min_feas*100:.4f}%."
        )

    if sigma_max > sigma_max_feas + 1e-6:
        raise FiberyLoaderError(
            f"Calibração '{calibration_name}': Sigma Max = {sigma_max*100:.4f}% "
            f"é inviável dado o universo. Vol máxima atingível = "
            f"{sigma_max_feas*100:.4f}%."
        )

    if sigma_max <= sigma_min:
        raise FiberyLoaderError(
            f"Calibração '{calibration_name}': Sigma Max ({sigma_max*100:.2f}%) "
            f"deve ser estritamente maior que Sigma Min ({sigma_min*100:.2f}%)."
        )

    # ── Montar SpectrumConfig
    config = SpectrumConfig(
        assets=assets,
        buckets=buckets,
        rc_targets=rc_targets,
        full_corr=full_corr,
        sigma_min_pct=sigma_min * 100.0,        # ← converte fração → %
        sigma_max_pct=sigma_max * 100.0,
        n_profiles=int(n_profiles),
        min_weight_threshold=float(min_weight_threshold),
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
        "id":                   ["fibery/id", "id", "Id"],
        "name":                 ["Inv-Rsrch-Quant/Name", "Name", "name"],
        "data_calibracao":      ["Inv-Rsrch-Quant/Data Calibração", "Data Calibração", "data_calibracao"],
        "status":               ["workflow/state", "State", "state", "Status"],
        "sigma_min":            ["Inv-Rsrch-Quant/Sigma Min", "Sigma Min", "sigma_min"],
        "sigma_max":            ["Inv-Rsrch-Quant/Sigma Max", "Sigma Max", "sigma_max"],
        "n_profiles":           ["Inv-Rsrch-Quant/N Profiles", "N Profiles", "n_profiles"],
        "min_weight_threshold": ["Inv-Rsrch-Quant/Min Weight Threshold", "Min Weight Threshold", "min_weight_threshold"],
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


def _read_corr_classes(calibration_name: str) -> pd.DataFrame:
    """Lê Correlação Cross-Classe (todos os pares). Filtra pela calibração."""
    try:
        df = read_fibery(DB_CORR_CLASSES)
    except Exception as exc:
        raise FiberyLoaderError(
            f"Falha ao ler '{DB_CORR_CLASSES}' do Fibery: {exc}"
        ) from exc

    df = _normalize_columns(df, {
        "calibration_name": ["Calibração", "calibracao_id"],
        "classe_a": ["Classe A", "classe_a"],
        "classe_b": ["Classe B", "classe_b"],
        "rho": ["Rho", "rho"],
    }, table_name=DB_CORR_CLASSES)

    return df[df["calibration_name"] == calibration_name].copy()

# ── Construção da matriz de correlação ─────────────────────────────────────────

def _build_full_corr_matrix(
    assets: list[AssetClass],
    corr_df: pd.DataFrame,
) -> np.ndarray:
    """
    Monta matriz n × n a partir dos pares cadastrados em DB_CORR_CLASSES,
    na ordem de `assets`. Diagonal = 1; par (j, i) é espelhado de (i, j).

    `corr_df` tem colunas: classe_a, classe_b, rho.

    Validação dura (fail-loud):
      • Todo par (i, j) com i < j deve estar presente.
      • Não pode haver duplicatas com rho conflitante.
      • Classes referenciadas devem existir no universo.
    """
    n = len(assets)
    name_to_idx = {a.name: i for i, a in enumerate(assets)}
    M = np.eye(n)

    # Conjunto de pares preenchidos (em forma normalizada: tupla ordenada)
    seen: dict[tuple[int, int], float] = {}

    for _, row in corr_df.iterrows():
        ca = row["classe_a"]
        cb = row["classe_b"]
        rho = float(row["rho"])

        ca_name = _resolve_relation_value(ca, assets)
        cb_name = _resolve_relation_value(cb, assets)

        if ca_name is None or cb_name is None:
            raise FiberyLoaderError(
                f"Não consegui resolver par de correlação: ({ca}, {cb})"
            )
        if ca_name not in name_to_idx or cb_name not in name_to_idx:
            raise FiberyLoaderError(
                f"Par de correlação referencia classe inexistente no universo: "
                f"('{ca_name}', '{cb_name}')"
            )

        i = name_to_idx[ca_name]
        j = name_to_idx[cb_name]
        if i == j:
            # Diagonal: deve ser 1.0 (tolerância numérica)
            if abs(rho - 1.0) > 1e-9:
                raise FiberyLoaderError(
                    f"Par diagonal ('{ca_name}','{cb_name}') tem rho={rho}, "
                    f"esperado 1.0"
                )
            continue

        key = (min(i, j), max(i, j))
        if key in seen and abs(seen[key] - rho) > 1e-9:
            raise FiberyLoaderError(
                f"Par ('{ca_name}','{cb_name}') aparece duplicado com rho "
                f"conflitante: {seen[key]} vs {rho}"
            )
        seen[key] = rho
        M[i, j] = rho
        M[j, i] = rho

    # ── Completude: todos os pares i<j devem estar presentes ────────────
    missing: list[tuple[str, str]] = []
    for i in range(n):
        for j in range(i + 1, n):
            if (i, j) not in seen:
                missing.append((assets[i].name, assets[j].name))

    if missing:
        preview = ", ".join(f"({a}, {b})" for a, b in missing[:10])
        more = f" ... e mais {len(missing) - 10}" if len(missing) > 10 else ""
        raise FiberyLoaderError(
            f"Correlações Cross-Classe incompletas: faltam {len(missing)} "
            f"pares no Fibery. Exemplos: {preview}{more}"
        )

    return M


# ── Cálculo da faixa de volatilidade viável ───────────────────────────────────

def _compute_feasible_vol_range(
    assets: list[AssetClass],
    full_corr: np.ndarray,
) -> tuple[float, float]:
    """
    Calcula a faixa de volatilidade viável do universo via QP long-only:
      sigma_min: min sqrt(w'Σw)  s.t. sum(w)=1, 0 ≤ w ≤ max_weight
      sigma_max: max sqrt(w'Σw)  s.t. mesmas constraints

    Retorna (sigma_min, sigma_max) em fração.
    """
    from scipy.optimize import minimize
    from ..core import _build_cov

    n = len(assets)
    vols = np.array([a.default_vol for a in assets])
    max_weights = np.array([a.max_weight for a in assets])
    cov = _build_cov(vols, full_corr)

    bounds = [(0.0, float(max_weights[i])) for i in range(n)]
    constraints = [{"type": "eq", "fun": lambda w: w.sum() - 1}]

    def vol_squared(w):
        return float(w @ cov @ w)

    # Min-variance: convexo, único ótimo
    res_min = minimize(
        vol_squared, np.ones(n) / n,
        method="SLSQP", bounds=bounds, constraints=constraints,
        options={"maxiter": 5_000, "ftol": 1e-14},
    )
    if not res_min.success:
        raise FiberyLoaderError(
            f"Falha ao calcular vol mínima viável: {res_min.message}"
        )
    sigma_min_feas = float(np.sqrt(max(res_min.fun, 0.0)))

    # Max-variance: não-convexo (maximizar função convexa). Tenta múltiplos
    # pontos iniciais — cada vértice de concentração e equal-weight.
    starts = [np.eye(n)[i] * max_weights[i] for i in range(n)]
    starts.append(np.ones(n) / n)

    best_max = 0.0
    for w0_raw in starts:
        w0 = w0_raw.copy()
        s = w0.sum()
        if s <= 0:
            continue
        w0 = w0 / s
        # Reclip para garantir bounds
        w0 = np.clip(w0, [b[0] for b in bounds], [b[1] for b in bounds])
        if w0.sum() <= 0:
            continue
        w0 = w0 / w0.sum()
        try:
            res = minimize(
                lambda w: -vol_squared(w), w0,
                method="SLSQP", bounds=bounds, constraints=constraints,
                options={"maxiter": 5_000, "ftol": 1e-14},
            )
            if res.success:
                v = float(np.sqrt(max(-res.fun, 0.0)))
                if v > best_max:
                    best_max = v
        except Exception:
            continue

    if best_max == 0.0:
        raise FiberyLoaderError("Falha ao calcular vol máxima viável do universo")

    return sigma_min_feas, best_max

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
