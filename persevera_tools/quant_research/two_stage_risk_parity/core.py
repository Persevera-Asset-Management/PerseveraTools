"""
Motor matemático do Espectro de Alocação Onshore — Persevera Asset Management.

Metodologia: Risk Budgeting paramétrico com targets de contribuição de risco
por bucket. Para cada perfil:
  • Define-se uma distribuição-alvo de RC entre buckets (interpolada
    linearmente entre os endpoints `p1` e `p10` da SpectrumConfig).
  • Distribui-se o RC-target uniformemente entre as classes de cada bucket
    (ou via `intra_rc_weights` se fornecido).
  • O otimizador encontra os pesos que produzem essa distribuição de risco
    com a vol-alvo do perfil.

Características:
  • Consome apenas SpectrumConfig (sem conhecer Fibery/banco/etc.)
  • Caixa tratado como decisão independente (não entra na otimização).
  • HRP de dois níveis computado para diagnóstico/comparação.
  • Constraint opcional de RC máximo por classe (campo `max_rc`).
"""

from __future__ import annotations
import warnings
from typing import Optional

import numpy as np
from scipy.optimize import minimize

from .config import SpectrumConfig


# ── Funções matemáticas básicas ───────────────────────────────────────────────

def _portfolio_vol(w: np.ndarray, cov: np.ndarray) -> float:
    return float(np.sqrt(max(w @ cov @ w, 0.0)))


def _risk_contrib(w: np.ndarray, cov: np.ndarray) -> np.ndarray:
    """Contribuições de risco absolutas (somam à vol do portfólio)."""
    pv = _portfolio_vol(w, cov)
    if pv == 0.0:
        return np.zeros_like(w)
    return w * (cov @ w) / pv


def _build_cov(vols: np.ndarray, corr: np.ndarray) -> np.ndarray:
    D = np.diag(vols)
    return D @ corr @ D


def _build_full_corr(
    bucket_indices: dict[str, list[int]],
    intra_corrs: dict[str, np.ndarray],
    macro_corr: np.ndarray,
) -> np.ndarray:
    """Combina blocos intra-bucket com correlação cross-bucket constante."""
    n_assets = sum(len(idxs) for idxs in bucket_indices.values())
    corr = np.eye(n_assets)
    bkeys = list(bucket_indices.keys())

    for a, bk_a in enumerate(bkeys):
        ia = bucket_indices[bk_a]
        corr[np.ix_(ia, ia)] = intra_corrs[bk_a]
        for b in range(a + 1, len(bkeys)):
            ib = bucket_indices[bkeys[b]]
            rho = float(macro_corr[a, b])
            corr[np.ix_(ia, ib)] = rho
            corr[np.ix_(ib, ia)] = rho

    return corr


def _vol_curve(sigma_min: float, sigma_max: float, n_profiles: int) -> np.ndarray:
    """Curva exponencial: σ(p) = σ_min · e^[k·(p−1)]."""
    if n_profiles == 1:
        return np.array([sigma_min])
    k = np.log(sigma_max / sigma_min) / (n_profiles - 1)
    return np.array([sigma_min * np.exp(k * p) for p in range(n_profiles)])


# ── HRP de dois níveis (para diagnóstico) ─────────────────────────────────────

def _rp_objective(w: np.ndarray, cov: np.ndarray) -> float:
    rc = _risk_contrib(w, cov)
    total = rc.sum()
    if total == 0.0:
        return 0.0
    rc_pct = rc / total
    target = np.ones(len(w)) / len(w)
    return float(np.sum((rc_pct - target) ** 2))


def _solve_rp(n: int, cov: np.ndarray, bounds=None) -> np.ndarray:
    if bounds is None:
        bounds = [(0.001, 1.0)] * n
    constraints = [{"type": "eq", "fun": lambda w: w.sum() - 1}]
    result = minimize(
        _rp_objective,
        np.ones(n) / n,
        args=(cov,),
        method="SLSQP",
        bounds=bounds,
        constraints=constraints,
        options={"maxiter": 10_000, "ftol": 1e-14},
    )
    if not result.success:
        warnings.warn(
            f"_solve_rp não convergiu: {result.message}",
            RuntimeWarning,
            stacklevel=2,
        )
    return result.x


def _compute_hrp(
    vols: np.ndarray,
    macro_corr: np.ndarray,
    intra_corrs: dict[str, np.ndarray],
    bucket_indices: dict[str, list[int]],
    intra_bounds: dict[str, Optional[float]],
) -> tuple[np.ndarray, np.ndarray]:
    """Retorna (w_hrp, w_macro) — usado apenas como referência diagnóstica."""
    bucket_keys = list(bucket_indices.keys())
    n_buckets = len(bucket_keys)
    n_assets = sum(len(v) for v in bucket_indices.values())

    w_bkts: dict[str, np.ndarray] = {}
    bvols = np.zeros(n_buckets)
    for k_idx, bkey in enumerate(bucket_keys):
        idxs = bucket_indices[bkey]
        v_bkt = vols[idxs]
        C_bkt = intra_corrs[bkey]
        COV_bkt = _build_cov(v_bkt, C_bkt)
        intra_max = intra_bounds.get(bkey)
        bnds = [(0.01, float(intra_max))] * len(idxs) if intra_max is not None else None
        w_bkt = _solve_rp(len(idxs), COV_bkt, bnds)
        w_bkts[bkey] = w_bkt
        bvols[k_idx] = _portfolio_vol(w_bkt, COV_bkt)

    COV_macro = _build_cov(bvols, macro_corr)
    w_macro = _solve_rp(n_buckets, COV_macro, [(0.05, 0.85)] * n_buckets)

    w_hrp = np.zeros(n_assets)
    for k_idx, bkey in enumerate(bucket_keys):
        idxs = bucket_indices[bkey]
        for j, idx in enumerate(idxs):
            w_hrp[idx] = w_macro[k_idx] * w_bkts[bkey][j]

    return w_hrp, w_macro


# ── Interpolação e expansão de RC targets ─────────────────────────────────────

def _interpolate_rc_targets(
    rc_p1: np.ndarray,
    rc_p10: np.ndarray,
    n_profiles: int,
) -> np.ndarray:
    """
    Para cada perfil p ∈ {1, ..., n_profiles}, calcula o RC-target por bucket
    interpolando linearmente no índice do perfil:
        rc_p[b] = (1 - f) * rc_p1[b] + f * rc_p10[b],  f = (p-1)/(n_profiles-1)

    Retorna matriz (n_profiles, n_buckets) com a soma de cada linha = 1.
    """
    if n_profiles == 1:
        return np.array([(rc_p1 + rc_p10) / 2])
    out = np.zeros((n_profiles, len(rc_p1)))
    for i in range(n_profiles):
        f = i / (n_profiles - 1)
        out[i] = (1 - f) * rc_p1 + f * rc_p10
    return out


def _expand_rc_to_assets(
    rc_target_bucket: np.ndarray,
    bucket_indices: dict[str, list[int]],
    intra_rc_weights: Optional[dict[str, np.ndarray]] = None,
) -> np.ndarray:
    """
    Distribui RC-target de cada bucket entre suas classes.
    Default: uniformemente. Se `intra_rc_weights[bkey]` fornecido, usa ele.
    """
    n_assets = sum(len(idxs) for idxs in bucket_indices.values())
    rc_asset = np.zeros(n_assets)
    for k_idx, (bkey, idxs) in enumerate(bucket_indices.items()):
        bucket_rc = float(rc_target_bucket[k_idx])
        if intra_rc_weights and bkey in intra_rc_weights:
            w_intra = np.asarray(intra_rc_weights[bkey], dtype=float)
            w_intra = w_intra / w_intra.sum()
        else:
            w_intra = np.ones(len(idxs)) / len(idxs)
        for j, idx in enumerate(idxs):
            rc_asset[idx] = bucket_rc * w_intra[j]
    return rc_asset


# ── Otimização por perfil: Risk Budgeting com constraint de Max RC ────────────

def _optimize_profile_rb(
    vol_target: float,
    cov: np.ndarray,
    rc_target_asset: np.ndarray,
    max_weights: np.ndarray,
    max_rc: np.ndarray,                  # (n,) com NaN onde não há limite
    bucket_indices: dict[str, list[int]],
    min_weight_threshold: float = 0.005,
) -> tuple[np.ndarray, bool]:
    """
    Encontra w tal que:
      • soma(w) = 1
      • vol(w) = vol_target  (igualdade)
      • 0 ≤ w_i ≤ max_weight_i
      • rc_pct_i(w) ≤ max_rc_i (onde definido)
      • Σ_i (rc_pct_i - rc_target_asset_i)^2 é mínima

    Retorna (w, converged).
    """
    n = len(rc_target_asset)
    has_max_rc = ~np.isnan(max_rc)

    def objective(w):
        pv = _portfolio_vol(w, cov)
        if pv <= 1e-12:
            return 1e6
        rc = w * (cov @ w) / pv
        rc_pct = rc / rc.sum()
        return float(np.sum((rc_pct - rc_target_asset) ** 2))

    constraints = [
        {"type": "eq", "fun": lambda w: w.sum() - 1},
        {"type": "eq", "fun": lambda w: _portfolio_vol(w, cov) - vol_target},
    ]
    # Adiciona constraint de RC máximo para cada classe que tem max_rc definido
    for i in range(n):
        if has_max_rc[i]:
            limit = float(max_rc[i])
            constraints.append({
                "type": "ineq",
                "fun": lambda w, idx=i, lim=limit: lim - _rc_pct_at(w, cov, idx),
            })

    bounds = [(0.0, float(max_weights[i])) for i in range(n)]

    # Pontos iniciais
    starts: list[np.ndarray] = []
    vols_diag = np.sqrt(np.diag(cov))
    if (vols_diag > 0).all():
        w0_rc = rc_target_asset / vols_diag
        if w0_rc.sum() > 0:
            starts.append(w0_rc / w0_rc.sum())
    starts.append(np.ones(n) / n)
    w0_bkt = np.zeros(n)
    for idxs in bucket_indices.values():
        bucket_rc = float(rc_target_asset[idxs].sum())
        for i in idxs:
            w0_bkt[i] = bucket_rc / len(idxs)
    if w0_bkt.sum() > 0:
        starts.append(w0_bkt / w0_bkt.sum())

    best_w, best_obj, converged = None, np.inf, False
    for w0_raw in starts:
        w0 = np.clip(w0_raw, [b[0] for b in bounds], [b[1] for b in bounds])
        if w0.sum() <= 0:
            continue
        w0 = w0 / w0.sum()
        try:
            res = minimize(
                objective, w0,
                method="SLSQP",
                bounds=bounds,
                constraints=constraints,
                options={"maxiter": 5_000, "ftol": 1e-12},
            )
            if res.success and res.fun < best_obj:
                best_obj = res.fun
                best_w = res.x.copy()
                converged = True
        except Exception as exc:
            warnings.warn(
                f"_optimize_profile_rb: falha — {exc}",
                RuntimeWarning,
                stacklevel=3,
            )

    if best_w is None:
        best_w = starts[0] if starts else np.ones(n) / n
        best_w = np.clip(best_w, 0.0, 1.0)
        if best_w.sum() > 0:
            best_w /= best_w.sum()

    best_w = np.clip(best_w, 0.0, 1.0)
    if best_w.sum() > 0:
        best_w /= best_w.sum()

    # ── Polish: zera pesos < threshold e RE-OTIMIZA no conjunto ativo ────
    active = best_w >= min_weight_threshold
    if active.sum() == 0:
        return best_w, converged

    polish_bounds = [
        (0.0, float(max_weights[i])) if active[i] else (0.0, 0.0)
        for i in range(n)
    ]
    polish_constraints = [
        {"type": "eq", "fun": lambda w: w.sum() - 1},
        {"type": "eq", "fun": lambda w: _portfolio_vol(w, cov) - vol_target},
    ]
    # Mantém constraint de max_rc no polish
    for i in range(n):
        if has_max_rc[i] and active[i]:
            limit = float(max_rc[i])
            polish_constraints.append({
                "type": "ineq",
                "fun": lambda w, idx=i, lim=limit: lim - _rc_pct_at(w, cov, idx),
            })

    w_start = best_w.copy()
    w_start[~active] = 0.0
    if w_start.sum() > 0:
        w_start /= w_start.sum()

    def polish_obj(w):
        return float(np.sum((w - best_w) ** 2))

    try:
        res_polish = minimize(
            polish_obj, w_start,
            method="SLSQP",
            bounds=polish_bounds,
            constraints=polish_constraints,
            options={"maxiter": 2_000, "ftol": 1e-14},
        )
        if res_polish.success:
            polished = np.clip(res_polish.x, 0.0, 1.0)
            polished[~active] = 0.0
            if polished.sum() > 0:
                polished /= polished.sum()
            best_w = polished
    except Exception as exc:
        warnings.warn(
            f"_optimize_profile_rb: polish falhou — {exc}",
            RuntimeWarning,
            stacklevel=3,
        )

    return best_w, converged


def _rc_pct_at(w: np.ndarray, cov: np.ndarray, idx: int) -> float:
    """Fração do risco contribuída pelo ativo `idx`."""
    pv = _portfolio_vol(w, cov)
    if pv <= 1e-12:
        return 0.0
    rc = w * (cov @ w) / pv
    total = rc.sum()
    if total <= 0:
        return 0.0
    return float(rc[idx] / total)


# ── Função principal ──────────────────────────────────────────────────────────

def build_spectrum(config: SpectrumConfig) -> dict:
    """
    Calcula o espectro de alocação a partir de uma SpectrumConfig.

    A config deve ter sido produzida por um loader (ex.: load_from_fibery)
    ou construída manualmente. Validação interna é executada antes do cálculo.

    Retorna dict com:
      classes               → list[str]         nomes das classes
      vols                  → np.ndarray        vols em decimal
      cov                   → np.ndarray        matriz de covariância
      vol_targets           → np.ndarray        vols-alvo em decimal (n_profiles,)
      weights               → dict[int→array]   pesos por perfil
      vol_realized          → dict[int→float]   vol realizada por perfil (%)
      risk_contrib          → dict[int→array]   contrib. de risco por perfil (fração)
      rc_target_bucket      → dict[int→array]   RC-target por bucket
      rc_realized_bucket    → dict[int→array]   RC realizado por bucket
      rc_target_asset       → dict[int→array]   RC-target por classe
      converged_per_profile → dict[int→bool]    True se o solver convergiu
      bucket_indices        → dict
      bucket_labels         → dict
      min_weight_threshold  → float
      w_hrp, w_macro, vol_hrp → diagnóstico HRP
      config                → SpectrumConfig    config usada (para auditoria)
    """
    config.validate()

    bucket_indices = config.bucket_indices
    bucket_keys = config.bucket_keys
    vols = config.vols_array

    # ── Matriz de covariância ────────────────────────────────────────────
    full_corr = _build_full_corr(bucket_indices, config.intra_corrs, config.macro_corr)
    cov = _build_cov(vols, full_corr)

    # ── HRP de referência (diagnóstico) ──────────────────────────────────
    w_hrp, w_macro = _compute_hrp(
        vols, config.macro_corr, config.intra_corrs,
        bucket_indices, config.intra_bounds,
    )
    vol_hrp = _portfolio_vol(w_hrp, cov) * 100.0

    # ── Curva de vols-alvo ───────────────────────────────────────────────
    vol_targets = _vol_curve(
        config.sigma_min_pct / 100.0,
        config.sigma_max_pct / 100.0,
        config.n_profiles,
    )

    # ── RC-targets por perfil ────────────────────────────────────────────
    rc_p1  = np.array([config.rc_targets[bk].rc_p1  for bk in bucket_keys])
    rc_p10 = np.array([config.rc_targets[bk].rc_p10 for bk in bucket_keys])
    rc_targets_per_profile = _interpolate_rc_targets(rc_p1, rc_p10, config.n_profiles)

    # ── Otimização por perfil ────────────────────────────────────────────
    weights: dict[int, np.ndarray] = {}
    vol_realized: dict[int, float] = {}
    risk_contrib: dict[int, np.ndarray] = {}
    rc_target_bucket: dict[int, np.ndarray] = {}
    rc_realized_bucket: dict[int, np.ndarray] = {}
    rc_target_asset: dict[int, np.ndarray] = {}
    converged_per_profile: dict[int, bool] = {}

    for p in range(1, config.n_profiles + 1):
        vt = float(vol_targets[p - 1])
        rc_tgt_bkt = rc_targets_per_profile[p - 1]
        rc_tgt_ast = _expand_rc_to_assets(
            rc_tgt_bkt, bucket_indices, config.intra_rc_weights
        )

        w, converged = _optimize_profile_rb(
            vol_target=vt,
            cov=cov,
            rc_target_asset=rc_tgt_ast,
            max_weights=config.max_weights_array,
            max_rc=config.max_rc_array,
            bucket_indices=bucket_indices,
            min_weight_threshold=config.min_weight_threshold,
        )

        weights[p] = w
        vol_realized[p] = _portfolio_vol(w, cov) * 100.0
        converged_per_profile[p] = converged

        rc = _risk_contrib(w, cov)
        rc_total = rc.sum()
        rc_pct = rc / rc_total if rc_total > 0 else rc
        risk_contrib[p] = rc_pct

        rc_real_bkt = np.array([
            float(rc_pct[idxs].sum()) for idxs in bucket_indices.values()
        ])
        rc_target_bucket[p]   = rc_tgt_bkt
        rc_target_asset[p]    = rc_tgt_ast
        rc_realized_bucket[p] = rc_real_bkt

    return {
        "classes":               config.class_names,
        "vols":                  vols,
        "cov":                   cov,
        "vol_targets":           vol_targets,
        "weights":               weights,
        "vol_realized":          vol_realized,
        "risk_contrib":          risk_contrib,
        "rc_target_bucket":      rc_target_bucket,
        "rc_target_asset":       rc_target_asset,
        "rc_realized_bucket":    rc_realized_bucket,
        "converged_per_profile": converged_per_profile,
        "bucket_indices":        bucket_indices,
        "bucket_labels":         config.bucket_labels,
        "min_weight_threshold":  config.min_weight_threshold,
        "w_hrp":                 w_hrp,
        "w_macro":               w_macro,
        "vol_hrp":               vol_hrp,
        "config":                config,
    }


def get_profile_summary(result: dict, profile: int) -> dict:
    """Retorna um resumo legível de um perfil específico."""
    w = result["weights"][profile]
    rc = result["risk_contrib"][profile]
    vt = result["vol_targets"][profile - 1] * 100.0
    vr = result["vol_realized"][profile]
    threshold = result.get("min_weight_threshold", 0.005)
    converged = result.get("converged_per_profile", {}).get(profile, True)

    bucket_keys = list(result["bucket_indices"].keys())
    rc_tgt_bkt  = result["rc_target_bucket"][profile]
    rc_real_bkt = result["rc_realized_bucket"][profile]

    bucket_weights = {
        bkey: float(sum(w[i] for i in idxs))
        for bkey, idxs in result["bucket_indices"].items()
    }
    bucket_rc = {
        bkey: {
            "target":   float(rc_tgt_bkt[k]),
            "realized": float(rc_real_bkt[k]),
            "delta":    float(rc_real_bkt[k] - rc_tgt_bkt[k]),
        }
        for k, bkey in enumerate(bucket_keys)
    }

    asset_weights = [
        {
            "class":        result["classes"][i],
            "bucket":       next(k for k, v in result["bucket_indices"].items() if i in v),
            "weight":       float(w[i]),
            "risk_contrib": float(rc[i]),
            "vol_hist":     float(result["vols"][i] * 100.0),
        }
        for i in range(len(result["classes"]))
        if w[i] >= threshold
    ]

    return {
        "profile":        profile,
        "vol_target":     vt,
        "vol_realized":   vr,
        "converged":      converged,
        "bucket_weights": bucket_weights,
        "bucket_rc":      bucket_rc,
        "asset_weights":  asset_weights,
    }
