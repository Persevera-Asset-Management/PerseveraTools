"""
Motor de cálculo do Espectro de Alocação Onshore — Persevera Asset Management

Metodologia:
  • Hierarchical Risk Parity (HRP) em dois níveis como portfólio âncora
  • Escala de perfis 1–10 via curva exponencial de volatilidade-alvo
  • Caixa tratado como decisão independente (não entra na otimização)
  • Cada perfil é otimizado individualmente: minimiza distância ao HRP
    sujeito à restrição de volatilidade-alvo

Uso:
  from persevera_tools.quant_research.hrp import build_spectrum
  result = build_spectrum(vols, corr_matrix)
  # result['weights'][p]        → array de pesos para o perfil p (1–10)
  # result['vol_realized'][p]   → volatilidade realizada do perfil p (%)
  # result['vol_targets']       → array de vols-alvo (decimal)
  # result['risk_contrib'][p]   → array de contribuições de risco (0–1)
  # result['w_hrp']             → pesos do portfólio HRP âncora
  # result['w_macro']           → pesos dos buckets macro no HRP
"""

from __future__ import annotations
import warnings

import numpy as np
from scipy.optimize import minimize

# ── CONSTANTES: CLASSES E BUCKETS ────────────────────────────────────────────
#
# `BUCKETS_CLASSES` é a fonte única da verdade para o universo de ativos.
# A ordem dos buckets e das classes dentro de cada bucket define a ordenação
# posicional usada por todas as matrizes/arrays do módulo (vols, cov,
# max_weights, âncoras, intra_corrs, macro_corr). NÃO altere a ordem sem
# revisar `_default_intra_corrs` e `DEFAULT_MACRO_CORR`.
#
# Campos por classe:
#   proxy       : identificador do ativo proxy (usado para puxar séries de
#                 dados externas; hoje hard-coded, futuramente carregado de
#                 fonte externa)
#   default_vol : volatilidade anualizada de referência (% a.a.)
#   max_weight  : limite superior de peso na otimização (fração)
#   w_p1        : peso na âncora conservadora (perfil 1)
#   w_p10       : peso na âncora agressiva (perfil 10)
#
# Campos opcionais por bucket:
#   intra_max_weight : limite superior de peso de cada classe DENTRO do
#                      portfólio RP intra-bucket (HRP nível 2). Útil para
#                      evitar concentração quando a estrutura intra-bucket
#                      tem poucos ativos (ex.: RV com 2 classes). Default:
#                      None (sem limite explícito além de [0.001, 1.0]).

#
# Sobre `w_p1` e `w_p10`:
#   Os pesos foram derivados das âncoras históricas de 15 classes via:
#     • somar GI + HY ao consolidar classes de crédito ("Inflação Crédito",
#       "Pós Crédito"),
#     • somar RVEC + RVES ao consolidar "RV EUA",
#     • absorver o peso da extinta classe "FIIs" na classe nova mais
#       próxima dinamicamente (RF Pós Crédito no perfil 1, RV Brasil no
#       perfil 10 — equity imobiliário ≈ equity amplo).
#   Cada coluna soma exatamente 1.0 por construção (sanity verificada
#   em `_validate_buckets_classes`); ainda assim `build_spectrum` faz
#   uma normalização defensiva em runtime.

BUCKETS_CLASSES: dict[str, dict] = {
    "RF": {
        "label": "Renda Fixa",
        "classes": {
            "RF Pré Curta":                 {"proxy": "anbima_irf_m1",      "default_vol":  0.679, "max_weight": 0.50, "w_p1": 0.350, "w_p10": 0.020},
            "RF Pré Longa":                 {"proxy": "anbima_irf_m1+",     "default_vol":  4.989, "max_weight": 0.20, "w_p1": 0.020, "w_p10": 0.030},
            "RF Inflação Curta":            {"proxy": "anbima_ima_b5",      "default_vol":  2.898, "max_weight": 0.30, "w_p1": 0.150, "w_p10": 0.015},
            "RF Inflação Longa":            {"proxy": "anbima_ima_b5+",     "default_vol": 10.440, "max_weight": 0.20, "w_p1": 0.020, "w_p10": 0.060},
            "RF Inflação Crédito":          {"proxy": "anbima_ida_ipca",    "default_vol":  4.403, "max_weight": 0.20, "w_p1": 0.110, "w_p10": 0.050},
            "RF Pós Crédito":               {"proxy": "anbima_ida_di",      "default_vol":  1.507, "max_weight": 0.40, "w_p1": 0.320, "w_p10": 0.010},
        },
    },
    "RV": {
        "label": "Renda Variável",
        "intra_max_weight": 0.60,
        "classes": {
            "RV Brasil":                    {"proxy": "br_ibovespa",        "default_vol": 21.538, "max_weight": 0.40, "w_p1": 0.000, "w_p10": 0.275},
            "RV EUA":                       {"proxy": "us_sp500",           "default_vol": 18.630, "max_weight": 0.30, "w_p1": 0.000, "w_p10": 0.300},
        },
    },
    "Alternativos": {
        "label": "Alternativos",
        "classes": {
            "Multimercado":                 {"proxy": "anbima_ihfa",        "default_vol":  3.817, "max_weight": 0.30, "w_p1": 0.020, "w_p10": 0.100},
            "Bitcoin":                      {"proxy": "bitcoin_usd",        "default_vol": 50.000, "max_weight": 0.12, "w_p1": 0.000, "w_p10": 0.090},
            "Ouro":                         {"proxy": "gold_100oz_futures", "default_vol": 15.969, "max_weight": 0.15, "w_p1": 0.010, "w_p10": 0.050},
        },
    },
}


def _flatten_buckets(bcs: dict[str, dict]) -> list[tuple[str, str, dict]]:
    """Itera (bucket_key, class_name, class_data) preservando ordem do dict."""
    return [
        (bkey, cname, cdata)
        for bkey, b in bcs.items()
        for cname, cdata in b["classes"].items()
    ]


def _build_bucket_indices(bcs: dict[str, dict]) -> dict[str, list[int]]:
    bi: dict[str, list[int]] = {}
    pos = 0
    for bkey, b in bcs.items():
        n = len(b["classes"])
        bi[bkey] = list(range(pos, pos + n))
        pos += n
    return bi


_FLAT = _flatten_buckets(BUCKETS_CLASSES)

CLASSES              = [cname for _, cname, _ in _FLAT]
N_ASSETS             = len(CLASSES)
BUCKET_INDICES       = _build_bucket_indices(BUCKETS_CLASSES)
BUCKET_LABELS        = {bkey: b["label"] for bkey, b in BUCKETS_CLASSES.items()}
DEFAULT_PROXIES      = {cname: cdata["proxy"] for _, cname, cdata in _FLAT}
DEFAULT_VOLS_PCT     = {cname: cdata["default_vol"] for _, cname, cdata in _FLAT}
DEFAULT_MAX_WEIGHTS  = np.array([cdata["max_weight"] for _, _, cdata in _FLAT])
DEFAULT_INTRA_BOUNDS = {
    bkey: b.get("intra_max_weight") for bkey, b in BUCKETS_CLASSES.items()
}
_W_P1_RAW            = np.array([cdata["w_p1"]  for _, _, cdata in _FLAT])
_W_P10_RAW           = np.array([cdata["w_p10"] for _, _, cdata in _FLAT])


def _default_intra_corrs() -> dict[str, np.ndarray]:
    """
    Matrizes de correlação intra-bucket padrão.
    A ordem das linhas/colunas segue a ordem das classes em
    `BUCKETS_CLASSES[bucket]["classes"]`.
    """
    # Ordem: Pré Curta, Pré Longa, Inflação Curta, Inflação Longa,
    #        Inflação Crédito, Pós Crédito
    C_rf = np.array([
        [1.00, 0.75, 0.35, 0.25, 0.23, 0.10],
        [0.75, 1.00, 0.45, 0.55, 0.35, 0.15],
        [0.35, 0.45, 1.00, 0.70, 0.58, 0.20],
        [0.25, 0.55, 0.70, 1.00, 0.58, 0.15],
        [0.23, 0.35, 0.58, 0.58, 1.00, 0.26],
        [0.10, 0.15, 0.20, 0.15, 0.26, 1.00],
    ])
    C_rv = np.array([
        [1.00, 0.45],
        [0.45, 1.00],
    ])
    # Ordem: Multimercado, Bitcoin, Ouro
    C_alt = np.array([
        [1.00, 0.10, 0.10],
        [0.10, 1.00, 0.20],
        [0.10, 0.20, 1.00],
    ])
    return {"RF": C_rf, "RV": C_rv, "Alternativos": C_alt}


# Correlações macro entre buckets — ordem segue BUCKETS_CLASSES (RF, RV, Alt)
DEFAULT_MACRO_CORR = np.array([
    [1.00, 0.20, 0.25],
    [0.20, 1.00, 0.45],
    [0.25, 0.45, 1.00],
])


def _validate_buckets_classes(bcs: dict[str, dict]) -> None:
    """Sanity checks executados no carregamento do módulo."""
    required_class_keys = {"proxy", "default_vol", "max_weight", "w_p1", "w_p10"}
    seen_proxies: dict[str, str] = {}
    for bkey, b in bcs.items():
        if "label" not in b or "classes" not in b:
            raise ValueError(f"Bucket '{bkey}' deve conter 'label' e 'classes'")
        if not b["classes"]:
            raise ValueError(f"Bucket '{bkey}' não pode estar vazio")
        if "intra_max_weight" in b:
            imw = b["intra_max_weight"]
            if imw is not None and not 0.0 < imw <= 1.0:
                raise ValueError(
                    f"intra_max_weight do bucket '{bkey}' deve estar em (0, 1] ou ser None, "
                    f"recebido {imw}"
                )
        for cname, cdata in b["classes"].items():
            missing = required_class_keys - cdata.keys()
            if missing:
                raise ValueError(
                    f"Classe '{cname}' (bucket '{bkey}') não tem os campos: {sorted(missing)}"
                )
            proxy = cdata["proxy"]
            if not isinstance(proxy, str) or not proxy.strip():
                raise ValueError(
                    f"proxy de '{cname}' deve ser string não-vazia, recebido {proxy!r}"
                )
            if proxy in seen_proxies:
                raise ValueError(
                    f"proxy '{proxy}' duplicado nas classes "
                    f"'{seen_proxies[proxy]}' e '{cname}'"
                )
            seen_proxies[proxy] = cname
            if not 0.0 < cdata["max_weight"] <= 1.0:
                raise ValueError(
                    f"max_weight de '{cname}' deve estar em (0, 1], recebido {cdata['max_weight']}"
                )
            for k in ("w_p1", "w_p10"):
                if not 0.0 <= cdata[k] <= 1.0:
                    raise ValueError(f"{k} de '{cname}' deve estar em [0, 1]")
            if cdata["default_vol"] <= 0:
                raise ValueError(f"default_vol de '{cname}' deve ser > 0")
    # Âncoras: soma > 0 (são renormalizadas em build_spectrum, mas
    # idealmente já somam 1.0 para serem alocações interpretáveis).
    if _W_P1_RAW.sum() <= 0 or _W_P10_RAW.sum() <= 0:
        raise ValueError("Soma das âncoras (w_p1, w_p10) deve ser > 0")
    for name, raw in [("w_p1", _W_P1_RAW), ("w_p10", _W_P10_RAW)]:
        s = float(raw.sum())
        if abs(s - 1.0) > 0.05:
            warnings.warn(
                f"Soma de {name} = {s:.4f} (longe de 1.0). Os valores serão "
                f"renormalizados em runtime, mas considere ajustar para 1.0 "
                f"para que a interpretação como alocação seja direta.",
                UserWarning,
                stacklevel=2,
            )


def _validate_intra_corrs_shapes(
    intra_corrs: dict[str, np.ndarray],
    bucket_indices: dict[str, list[int]],
) -> None:
    """Verifica que cada matriz intra-bucket tem shape compatível com o bucket."""
    for bkey, idxs in bucket_indices.items():
        if bkey not in intra_corrs:
            raise ValueError(f"_default_intra_corrs() não contém o bucket '{bkey}'")
        expected = (len(idxs), len(idxs))
        actual = intra_corrs[bkey].shape
        if actual != expected:
            raise ValueError(
                f"_default_intra_corrs()['{bkey}'] tem shape {actual}, "
                f"esperado {expected} (n_classes do bucket '{bkey}' em BUCKETS_CLASSES). "
                f"Atualize a matriz para refletir a quantidade atual de classes."
            )


_validate_buckets_classes(BUCKETS_CLASSES)
_validate_intra_corrs_shapes(_default_intra_corrs(), BUCKET_INDICES)


# ── FUNÇÕES MATEMÁTICAS ───────────────────────────────────────────────────────

def _portfolio_vol(w: np.ndarray, cov: np.ndarray) -> float:
    return float(np.sqrt(w @ cov @ w))


def _risk_contrib(w: np.ndarray, cov: np.ndarray) -> np.ndarray:
    pv = _portfolio_vol(w, cov)
    if pv == 0.0:
        return np.zeros_like(w)
    return w * (cov @ w) / pv


def _rp_objective(w: np.ndarray, cov: np.ndarray) -> float:
    rc = _risk_contrib(w, cov)
    total = rc.sum()
    if total == 0.0:
        return 0.0
    rc_pct = rc / total
    target = np.ones(len(w)) / len(w)
    return float(np.sum((rc_pct - target) ** 2))


def _solve_rp(
    n: int,
    cov: np.ndarray,
    bounds: list[tuple] | None = None,
) -> np.ndarray:
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


def _build_cov(vols: np.ndarray, corr: np.ndarray) -> np.ndarray:
    D = np.diag(vols)
    return D @ corr @ D


def _validate_corr_psd(corr: np.ndarray, name: str = "corr") -> None:
    """
    Verifica simetria e positividade semi-definida.

    Usa tolerância relativa para PSD: `min_eig < -tol`, onde
    `tol = max(1e-8, 1e-10 * max(|eigs|))`. Isso evita falsos positivos
    de PSD em matrizes grandes onde o ruído numérico de `eigvalsh` pode
    produzir autovalores ligeiramente negativos (~1e-12) sem haver
    inconsistência real.
    """
    if not np.allclose(corr, corr.T, atol=1e-8):
        raise ValueError(f"{name} não é simétrica")
    eigs = np.linalg.eigvalsh(corr)
    min_eig = float(eigs.min())
    max_abs = float(np.abs(eigs).max())
    tol = max(1e-8, 1e-10 * max_abs)
    if min_eig < -tol:
        raise ValueError(
            f"{name} não é positiva semi-definida (menor autovalor: {min_eig:.6e}, "
            f"tolerância: {tol:.6e})"
        )


def _build_full_corr(
    bucket_indices: dict[str, list[int]],
    intra_corrs: dict[str, np.ndarray],
    macro_corr: np.ndarray | None = None,
) -> np.ndarray:
    """
    Constrói uma matriz de correlação completa n×n a partir de:
      • intra_corrs[bucket]    → estrutura completa dentro de cada bucket
      • macro_corr[a, b]       → correlação entre os buckets a e b
                                 (constante para todos os pares cross-bucket)

    Se `macro_corr` for None, a correlação cross-bucket é zero (bloco-diagonal).
    """
    n_assets = sum(len(idxs) for idxs in bucket_indices.values())
    corr = np.eye(n_assets)
    bkeys = list(bucket_indices.keys())

    for a, bk_a in enumerate(bkeys):
        ia = bucket_indices[bk_a]
        corr[np.ix_(ia, ia)] = intra_corrs[bk_a]

        if macro_corr is None:
            continue
        for b in range(a + 1, len(bkeys)):
            ib = bucket_indices[bkeys[b]]
            rho = float(macro_corr[a, b])
            corr[np.ix_(ia, ib)] = rho
            corr[np.ix_(ib, ia)] = rho

    return corr


def _vol_curve(sigma_min: float, sigma_max: float, n_profiles: int = 10) -> np.ndarray:
    """Curva exponencial: σ(p) = σ_min · e^[k·(p−1)]"""
    if n_profiles == 1:
        return np.array([sigma_min])
    k = np.log(sigma_max / sigma_min) / (n_profiles - 1)
    return np.array([sigma_min * np.exp(k * p) for p in range(n_profiles)])


# ── HRP EM DOIS NÍVEIS ────────────────────────────────────────────────────────

def _compute_hrp(
    vols: np.ndarray,
    macro_corr: np.ndarray,
    intra_corrs: dict[str, np.ndarray],
    bucket_indices: dict[str, list[int]] | None = None,
    intra_bounds: dict[str, float | None] | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Retorna (w_hrp, w_macro) onde:
      w_hrp   → pesos de todos os ativos no portfólio HRP
      w_macro → pesos dos buckets no nível macro

    `intra_bounds[bkey]` (opcional) define o peso máximo de cada classe dentro
    do RP intra-bucket. Se None ou ausente, usa o default `(0.001, 1.0)` do
    `_solve_rp`.
    """
    _bkt_idxs = bucket_indices if bucket_indices is not None else BUCKET_INDICES
    _intra_bounds = intra_bounds or {}
    bucket_keys = list(_bkt_idxs.keys())
    n_buckets = len(bucket_keys)
    n_assets = sum(len(v) for v in _bkt_idxs.values())

    # Nível 2 primeiro: RP intra-bucket (independente dos pesos macro)
    w_bkts: dict[str, np.ndarray] = {}
    bvols = np.zeros(n_buckets)
    for k_idx, bkey in enumerate(bucket_keys):
        idxs = _bkt_idxs[bkey]
        v_bkt = vols[idxs]
        C_bkt = intra_corrs[bkey]
        COV_bkt = _build_cov(v_bkt, C_bkt)

        intra_max = _intra_bounds.get(bkey)
        bnds = [(0.01, float(intra_max))] * len(idxs) if intra_max is not None else None
        w_bkt = _solve_rp(len(idxs), COV_bkt, bnds)
        w_bkts[bkey] = w_bkt
        # Vol realizada do portfólio RP intra-bucket como representante do bucket
        bvols[k_idx] = _portfolio_vol(w_bkt, COV_bkt)

    # Nível 1: RP entre buckets usando a vol real de cada portfólio intra-bucket
    COV_macro = _build_cov(bvols, macro_corr)
    w_macro = _solve_rp(n_buckets, COV_macro, [(0.05, 0.85)] * n_buckets)

    # Combina pesos macro e intra-bucket
    w_hrp = np.zeros(n_assets)
    for k_idx, bkey in enumerate(bucket_keys):
        idxs = _bkt_idxs[bkey]
        w_bkt = w_bkts[bkey]
        for j, idx in enumerate(idxs):
            w_hrp[idx] = w_macro[k_idx] * w_bkt[j]

    return w_hrp, w_macro


# ── OTIMIZAÇÃO POR PERFIL ─────────────────────────────────────────────────────

def _optimize_profile(
    vol_target: float,
    cov: np.ndarray,
    w_hrp: np.ndarray,
    vol_hrp: float,
    w_p1: np.ndarray,
    w_p10: np.ndarray,
    vol_p10: float,
    max_weights: np.ndarray,
    min_weight_threshold: float = 0.005,
    vol_min: float = 0.01,
    risk_concentration_penalty: float = 5.0,
) -> tuple[np.ndarray, bool]:
    """
    Para um vol_target dado, encontra o portfólio mais próximo ao HRP
    que atinge exatamente aquela volatilidade.

    A função objetivo é Σᵢ (wᵢ − wᵢ_ref)² · (1 + λ·wᵢ_ref), onde λ é
    `risk_concentration_penalty`. Esse fator penaliza mais desvios em
    ativos com peso de referência alto (mantém os "core holdings" próximos
    do alvo). λ=5 é o default heurístico; aumentar reforça o ancoramento.

    A constraint de vol é uma igualdade (`_portfolio_vol(w, cov) == vol_target`),
    que define uma superfície não-convexa. SLSQP resolve mas pode falhar em
    casos extremos. Após a otimização, pesos abaixo de `min_weight_threshold`
    são zerados e a soma é renormalizada para 1 — esse passo de pós-processamento
    pode mover ligeiramente a vol final do portfólio para fora de `vol_target`
    (verificável via `vol_realized` no resultado).

    Retorna
    -------
    (w, converged) : tuple
        w         : pesos finais (n,) com soma 1.
        converged : True se ao menos um ponto inicial convergiu (`res.success`),
                    False se todas as tentativas falharam (cai no fallback w_ref).
    """
    # Guard: âncora agressiva precisa ter vol estritamente maior que HRP
    if vol_p10 <= vol_hrp:
        warnings.warn(
            f"_optimize_profile: vol_p10 ({vol_p10:.4f}) ≤ vol_hrp ({vol_hrp:.4f}) — "
            f"interpolação acima do HRP indefinida; usando HRP como teto.",
            RuntimeWarning,
            stacklevel=3,
        )
        vol_p10 = vol_hrp + 1e-6  # pequeno épsilon para evitar div-zero

    # Portfólio de referência por interpolação linear
    if vol_target <= vol_hrp:
        denom = vol_hrp - vol_min
        frac = np.clip((vol_target - vol_min) / denom, 0.0, 1.0) if denom > 0 else 1.0
        w_ref = (1 - frac) * w_p1 + frac * w_hrp
    else:
        frac = np.clip((vol_target - vol_hrp) / (vol_p10 - vol_hrp), 0.0, 1.0)
        w_ref = (1 - frac) * w_hrp + frac * w_p10

    w_ref = np.clip(w_ref, 0.0, 1.0)
    w_ref /= w_ref.sum()

    def objective(w):
        return float(np.sum((w - w_ref) ** 2 * (1 + risk_concentration_penalty * w_ref)))

    constraints = [
        {"type": "eq", "fun": lambda w: w.sum() - 1},
        {"type": "eq", "fun": lambda w: _portfolio_vol(w, cov) - vol_target},
    ]
    bounds = [(0.0, float(max_weights[i])) for i in range(len(max_weights))]

    best_w, best_obj, converged = None, np.inf, False
    for w0_raw in [w_ref, w_hrp, w_p1, w_p10]:
        w0 = np.clip(w0_raw, [b[0] for b in bounds], [b[1] for b in bounds])
        w0 /= w0.sum()
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
                f"_optimize_profile: falha na otimização com ponto inicial — {exc}",
                RuntimeWarning,
                stacklevel=3,
            )

    if best_w is None:
        best_w = w_ref.copy()
        converged = False

    best_w = np.clip(best_w, 0.0, 1.0)
    best_w /= best_w.sum()

    # Pós-processamento: zera pesos < threshold e renormaliza.
    # Isso pode causar pequeno desvio entre vol_realized e vol_target.
    best_w[best_w < min_weight_threshold] = 0.0
    if best_w.sum() > 0:
        best_w /= best_w.sum()

    return best_w, converged


# ── FUNÇÃO PRINCIPAL ──────────────────────────────────────────────────────────

def build_spectrum(
    vols_pct: dict[str, float] | None = None,
    corr_matrix: np.ndarray | None = None,
    macro_corr: np.ndarray | None = None,
    intra_corrs: dict[str, np.ndarray] | None = None,
    sigma_min_pct: float = 1.0,
    sigma_max_pct: float = 12.0,
    max_weights: np.ndarray | None = None,
    min_weight_threshold: float = 0.005,
    n_profiles: int = 10,
    classes: list[str] | None = None,
    bucket_indices: dict[str, list[int]] | None = None,
    bucket_labels: dict[str, str] | None = None,
    intra_bounds: dict[str, float | None] | None = None,
    w_anchor_conservative: np.ndarray | None = None,
    w_anchor_aggressive: np.ndarray | None = None,
    risk_concentration_penalty: float = 5.0,
) -> dict:
    """
    Calcula o espectro completo de alocação.

    Universo padrão (definido em `BUCKETS_CLASSES`): 11 classes em 3 buckets,
    layout {RF: [0-5], RV: [6-7], Alternativos: [8-10]}. A ordem em
    `BUCKETS_CLASSES` é a fonte da verdade — `BUCKET_INDICES` é derivado dela.

    LIMITAÇÃO DO MODELO DE COVARIÂNCIA PADRÃO
    -----------------------------------------
    Quando `corr_matrix` não é fornecida, a covariância é montada como:
      • estrutura intra-bucket completa (via `intra_corrs[bucket]`), e
      • correlação cross-bucket CONSTANTE por par-de-bucket (via `macro_corr`).
    Ou seja, todo par (RF × RV) recebe a mesma correlação, independente de
    quais classes específicas. Para o universo padrão isso é uma simplificação
    consciente: na realidade, "RF Inflação Longa × Ibovespa" tem dinâmica
    bem diferente de "RF Pós Crédito × Ibovespa". Se essa precisão importar
    para o seu caso, passe sua própria `corr_matrix` n×n com a estrutura
    cross-bucket por-par-de-classe que desejar.

    Parâmetros
    ----------
    vols_pct : dict {classe: vol em % a.a.}
        Volatilidades anualizadas. Se None, usa os defaults definidos em
        `BUCKETS_CLASSES`.
    corr_matrix : np.ndarray (n×n)
        Matriz de correlação global. Se fornecida, é usada diretamente para
        a covariância (e pula a montagem intra+macro). Se None, a corr global
        é construída a partir de `intra_corrs` (estrutura intra-bucket) +
        `macro_corr` (correlação constante entre buckets). A matriz montada
        é validada (simétrica e PSD) antes de ser usada.
    macro_corr : np.ndarray (k×k, k = nº de buckets)
        Correlações entre buckets, usadas pelo HRP nível 1 (Risk Parity
        macro). Se None:
          • se `corr_matrix` foi fornecida, deriva como média dos blocos
            cross-bucket — mantém o HRP consistente com a covariância;
          • senão, usa `DEFAULT_MACRO_CORR` (3×3) quando o universo padrão
            é usado ou há exatamente 3 buckets;
          • caso contrário, identidade.
    intra_corrs : dict {bucket: np.ndarray}
        Correlações intra-bucket. Se None e usando o universo padrão, usa
        `_default_intra_corrs()`. Se None com classes customizadas, usa
        identidade por bucket (com aviso).
    sigma_min_pct : float
        Volatilidade-alvo do perfil 1 (% a.a.). Default: 1.0
    sigma_max_pct : float
        Volatilidade-alvo do perfil 10 (% a.a.). Default: 12.0
    max_weights : np.ndarray (n,)
        Limite máximo de peso por ativo. Se None, usa os defaults do
        universo padrão (`DEFAULT_MAX_WEIGHTS`), ou 1.0 para cada ativo em
        universos customizados.
    min_weight_threshold : float
        Pesos abaixo deste valor são zerados após a otimização e a soma é
        renormalizada para 1. Por causa dessa renormalização, `vol_realized`
        pode se desviar ligeiramente de `vol_targets` — isso NÃO indica falha
        do solver. Default: 0.005 (0,5%).
    n_profiles : int
        Número de perfis. Default: 10.
    classes : list[str]
        Nomes das classes de ativos. Se None, usa o universo padrão.
    bucket_indices : dict {bucket: list[int]}
        Mapeamento bucket → índices dos ativos em `classes`. Se None, usa o
        padrão {RF: [0-5], RV: [6-7], Alternativos: [8-10]}.
    bucket_labels : dict {bucket: str}
        Rótulos descritivos dos buckets. Se None, usa os defaults.
    intra_bounds : dict {bucket: float | None}
        Peso máximo por classe DENTRO do RP intra-bucket (HRP nível 2). Se
        None, usa `DEFAULT_INTRA_BOUNDS` no universo padrão (RV=0.60), ou
        sem limite explícito em universos customizados.
    w_anchor_conservative : np.ndarray (n,)
        Portfólio-âncora para interpolar o perfil mais conservador.
        Se None, usa `_W_P1_RAW` para o universo padrão, ou pesos iguais
        para universos customizados.
    w_anchor_aggressive : np.ndarray (n,)
        Portfólio-âncora para interpolar o perfil mais agressivo.
        Se None, usa `_W_P10_RAW` para o universo padrão, ou pesos iguais
        para universos customizados. Deve ter vol > vol_hrp.
    risk_concentration_penalty : float
        Coeficiente λ da função objetivo Σ (wᵢ − wᵢ_ref)² · (1 + λ·wᵢ_ref).
        Penaliza mais desvios em ativos com peso de referência alto.
        Default: 5.0.

    Retorna
    -------
    dict com chaves:
      classes               → list[str]         nomes das classes
      vols                  → np.ndarray        vols em decimal
      cov                   → np.ndarray        matriz de covariância (n×n)
      vol_targets           → np.ndarray        vols-alvo em decimal (n_profiles,)
      w_hrp                 → np.ndarray        pesos HRP (n,)
      w_macro               → np.ndarray        pesos macro por bucket (k,)
      vol_hrp               → float             vol do portfólio HRP (%)
      weights               → dict[int→array]   pesos por perfil
      vol_realized          → dict[int→float]   vol realizada por perfil (%)
      risk_contrib          → dict[int→array]   contrib. de risco por perfil (fração)
      converged_per_profile → dict[int→bool]    True se o solver convergiu para o perfil
      bucket_indices        → dict              mapeamento bucket → índices
      bucket_labels         → dict              rótulos dos buckets
      min_weight_threshold  → float             threshold usado para zerar pesos
    """
    # ── Resolve universo de ativos ────────────────────────────────────────────
    _using_default_universe = classes is None
    _classes = CLASSES if classes is None else list(classes)
    _bucket_indices = BUCKET_INDICES if bucket_indices is None else bucket_indices
    _bucket_labels = BUCKET_LABELS if bucket_labels is None else bucket_labels
    n_assets = len(_classes)
    n_buckets = len(_bucket_indices)

    # ── Validação de entradas ─────────────────────────────────────────────────
    if sigma_min_pct >= sigma_max_pct:
        raise ValueError(
            f"sigma_min_pct ({sigma_min_pct}) deve ser menor que sigma_max_pct ({sigma_max_pct})"
        )
    if n_profiles < 1:
        raise ValueError(f"n_profiles deve ser >= 1, recebido {n_profiles}")

    all_bucket_idxs = [i for idxs in _bucket_indices.values() for i in idxs]
    if sorted(all_bucket_idxs) != list(range(n_assets)):
        raise ValueError(
            "bucket_indices deve cobrir exatamente os índices 0 a n_assets-1, sem lacunas ou repetições"
        )

    if vols_pct is not None:
        missing = [c for c in _classes if c not in vols_pct]
        if missing:
            raise ValueError(f"vols_pct não contém as seguintes classes: {missing}")

    if corr_matrix is not None:
        if corr_matrix.shape != (n_assets, n_assets):
            raise ValueError(
                f"corr_matrix deve ter shape ({n_assets}, {n_assets}), recebido {corr_matrix.shape}"
            )
        _validate_corr_psd(corr_matrix, name="corr_matrix")

    if macro_corr is not None and macro_corr.shape != (n_buckets, n_buckets):
        raise ValueError(
            f"macro_corr deve ter shape ({n_buckets}, {n_buckets}), recebido {macro_corr.shape}"
        )

    if intra_corrs is not None:
        for bkey, idxs in _bucket_indices.items():
            if bkey not in intra_corrs:
                raise ValueError(f"intra_corrs não contém o bucket '{bkey}'")
            expected = (len(idxs), len(idxs))
            if intra_corrs[bkey].shape != expected:
                raise ValueError(
                    f"intra_corrs['{bkey}'] deve ter shape {expected}, "
                    f"recebido {intra_corrs[bkey].shape}"
                )

    if max_weights is not None and len(max_weights) != n_assets:
        raise ValueError(
            f"max_weights deve ter {n_assets} elementos, recebido {len(max_weights)}"
        )
    if w_anchor_conservative is not None and len(w_anchor_conservative) != n_assets:
        raise ValueError(
            f"w_anchor_conservative deve ter {n_assets} elementos"
        )
    if w_anchor_aggressive is not None and len(w_anchor_aggressive) != n_assets:
        raise ValueError(
            f"w_anchor_aggressive deve ter {n_assets} elementos"
        )

    # ── Volatilidades ─────────────────────────────────────────────────────────
    if vols_pct is not None:
        vols = np.array([vols_pct[c] for c in _classes]) / 100.0
    elif _using_default_universe:
        vols = np.array([DEFAULT_VOLS_PCT[c] for c in _classes]) / 100.0
    else:
        raise ValueError(
            "vols_pct é obrigatório quando classes customizadas são fornecidas"
        )

    # ── Matriz de covariância ─────────────────────────────────────────────────
    # Resolve correlações intra e macro a partir das entradas (com defaults).
    if corr_matrix is not None:
        _intra = {bkey: corr_matrix[np.ix_(idxs, idxs)] for bkey, idxs in _bucket_indices.items()}
    elif intra_corrs is not None:
        _intra = intra_corrs
    elif _using_default_universe:
        _intra = _default_intra_corrs()
    else:
        _intra = {bkey: np.eye(len(idxs)) for bkey, idxs in _bucket_indices.items()}
        warnings.warn(
            "intra_corrs não fornecida para classes customizadas — usando correlação zero entre ativos.",
            UserWarning,
            stacklevel=2,
        )

    if macro_corr is not None:
        _macro = macro_corr
    elif corr_matrix is not None:
        # Deriva da corr_matrix global (média dos blocos cross-bucket) para
        # manter HRP nível 1 consistente com a covariância principal.
        bkeys = list(_bucket_indices.keys())
        _macro = np.eye(n_buckets)
        for a, ka in enumerate(bkeys):
            ia = _bucket_indices[ka]
            for b in range(a + 1, n_buckets):
                ib = _bucket_indices[bkeys[b]]
                rho = float(corr_matrix[np.ix_(ia, ib)].mean())
                _macro[a, b] = rho
                _macro[b, a] = rho
    elif _using_default_universe:
        _macro = DEFAULT_MACRO_CORR
    elif n_buckets == 3:
        _macro = DEFAULT_MACRO_CORR
    else:
        _macro = np.eye(n_buckets)

    # Se o usuário forneceu corr_matrix global, usa-a diretamente.
    # Senão, constrói a partir de intra + macro (modelo: estrutura intra
    # completa + correlação macro constante entre buckets) e valida PSD —
    # combinações patológicas de correlações intra altas + macro altas
    # podem violar PSD.
    if corr_matrix is not None:
        cov = _build_cov(vols, corr_matrix)
    else:
        full_corr = _build_full_corr(_bucket_indices, _intra, _macro)
        _validate_corr_psd(full_corr, name="correlação montada (intra+macro)")
        cov = _build_cov(vols, full_corr)

    # ── Bounds intra-bucket para o HRP nível 2 ────────────────────────────────
    if intra_bounds is not None:
        _intra_bounds = intra_bounds
    elif _using_default_universe:
        _intra_bounds = DEFAULT_INTRA_BOUNDS
    else:
        _intra_bounds = None
        # Avisa se há intra_max_weight definidos no módulo que o usuário
        # pode estar perdendo silenciosamente ao customizar o universo.
        defaults_set = {k: v for k, v in DEFAULT_INTRA_BOUNDS.items() if v is not None}
        if defaults_set:
            warnings.warn(
                f"Universo customizado sem intra_bounds — DEFAULT_INTRA_BOUNDS "
                f"({defaults_set}) NÃO será aplicado. Passe intra_bounds explicitamente "
                f"se quiser limitar pesos dentro do RP intra-bucket.",
                UserWarning,
                stacklevel=2,
            )

    # ── HRP ───────────────────────────────────────────────────────────────────
    w_hrp, w_macro = _compute_hrp(vols, _macro, _intra, _bucket_indices, _intra_bounds)
    vol_hrp = _portfolio_vol(w_hrp, cov) * 100.0

    # ── Curva exponencial de vols-alvo ────────────────────────────────────────
    vol_targets = _vol_curve(sigma_min_pct / 100.0, sigma_max_pct / 100.0, n_profiles)

    # ── Portfólios âncora ─────────────────────────────────────────────────────
    if w_anchor_conservative is not None:
        w_p1 = w_anchor_conservative / w_anchor_conservative.sum()
    elif _using_default_universe:
        w_p1 = _W_P1_RAW / _W_P1_RAW.sum()
    else:
        w_p1 = np.ones(n_assets) / n_assets

    if w_anchor_aggressive is not None:
        w_p10 = w_anchor_aggressive / w_anchor_aggressive.sum()
    elif _using_default_universe:
        w_p10 = _W_P10_RAW / _W_P10_RAW.sum()
    else:
        w_p10 = np.ones(n_assets) / n_assets

    vol_p10 = _portfolio_vol(w_p10, cov) * 100.0

    # Sanity check: âncora agressiva precisa ter vol > HRP para a interpolação
    # do trecho superior fazer sentido.
    if vol_p10 <= vol_hrp:
        warnings.warn(
            f"vol da âncora agressiva ({vol_p10:.2f}%) ≤ vol do HRP "
            f"({vol_hrp:.2f}%). Perfis com vol_target > vol_hrp ficarão clipados.",
            UserWarning,
            stacklevel=2,
        )

    # ── Pesos máximos ─────────────────────────────────────────────────────────
    if max_weights is not None:
        _max_w = max_weights
    elif _using_default_universe:
        _max_w = DEFAULT_MAX_WEIGHTS
    else:
        _max_w = np.ones(n_assets)

    # ── Otimização por perfil ─────────────────────────────────────────────────
    weights: dict[int, np.ndarray] = {}
    vol_realized: dict[int, float] = {}
    risk_contrib: dict[int, np.ndarray] = {}
    converged_per_profile: dict[int, bool] = {}

    for p in range(1, n_profiles + 1):
        vt = float(vol_targets[p - 1])
        w, converged = _optimize_profile(
            vol_target=vt,
            cov=cov,
            w_hrp=w_hrp,
            vol_hrp=vol_hrp / 100.0,
            w_p1=w_p1,
            w_p10=w_p10,
            vol_p10=vol_p10 / 100.0,
            max_weights=_max_w,
            min_weight_threshold=min_weight_threshold,
            vol_min=sigma_min_pct / 100.0,
            risk_concentration_penalty=risk_concentration_penalty,
        )
        weights[p] = w
        vol_realized[p] = _portfolio_vol(w, cov) * 100.0
        converged_per_profile[p] = converged

        rc = _risk_contrib(w, cov)
        rc_total = rc.sum()
        risk_contrib[p] = rc / rc_total if rc_total > 0 else rc

    return {
        "classes":               _classes,
        "vols":                  vols,
        "cov":                   cov,
        "vol_targets":           vol_targets,
        "w_hrp":                 w_hrp,
        "w_macro":                w_macro,
        "vol_hrp":               vol_hrp,
        "weights":               weights,
        "vol_realized":          vol_realized,
        "risk_contrib":          risk_contrib,
        "converged_per_profile": converged_per_profile,
        "bucket_indices":        _bucket_indices,
        "bucket_labels":         _bucket_labels,
        "min_weight_threshold":  min_weight_threshold,
    }


def get_profile_summary(result: dict, profile: int) -> dict:
    """
    Retorna um resumo legível de um perfil específico.

    Filtra ativos com peso abaixo do `min_weight_threshold` registrado em
    `result` (mesmo threshold usado pelo otimizador para zerar pesos), de
    forma que o resumo só liste posições efetivamente alocadas.

    Exemplo:
        summary = get_profile_summary(result, profile=5)
        summary['bucket_weights']    → {'RF': 0.62, 'RV': 0.11, 'Alternativos': 0.27}
        summary['asset_weights']     → [{'class': ..., 'weight': ..., ...}]
        summary['vol_target']        → 3.01  (%)
        summary['vol_realized']      → 3.01  (%)
        summary['converged']         → True/False
    """
    w = result["weights"][profile]
    rc = result["risk_contrib"][profile]
    vt = result["vol_targets"][profile - 1] * 100.0
    vr = result["vol_realized"][profile]
    threshold = result.get("min_weight_threshold", 0.005)
    converged = result.get("converged_per_profile", {}).get(profile, True)

    bucket_weights = {
        bkey: float(sum(w[i] for i in idxs))
        for bkey, idxs in result["bucket_indices"].items()
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
        "asset_weights":  asset_weights,
    }

if __name__ == "__main__":
    result = build_spectrum(
        n_profiles=100,
        classes=[
            "RF Pré Curta", "RF Pré Longa",
            "RF Inflação Curta", "RF Inflação Longa",
            "RF Inflação Crédito", "RF Pós Crédito",
        ],
        bucket_indices={"RF": [0, 1, 2, 3, 4, 5]},
        bucket_labels={"RF": "Renda Fixa"},
        vols_pct={
            "RF Pré Curta":        8.5,
            "RF Pré Longa":       22.0,
            "RF Inflação Curta":  18.0,
            "RF Inflação Longa":  18.0,
            "RF Inflação Crédito": 18.0,
            "RF Pós Crédito":     18.0,
        },
    )

    summary = get_profile_summary(result, profile=5)
    print(summary)