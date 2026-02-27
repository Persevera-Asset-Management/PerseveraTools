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

CLASSES = [
    "RF Pré Curta (IRF-M 1)",
    "RF Pré Longa (IRF-M 1+)",
    "RF Inflação Curta (IMA-B 5)",
    "RF Inflação Longa (IMA-B 5+)",
    "RF Inflação Deb. GI",
    "RF Inflação Deb. HY",
    "RF Pós Crédito GI",
    "RF Pós Crédito HY",
    "Multimercado (IHFA)",
    "Ouro",
    "FIIs (IFIX)",
    "RV Brasil (Ibovespa)",
    "RV EUA c/ câmbio",
    "RV EUA s/ câmbio",
    "Bitcoin",
]

N_ASSETS = len(CLASSES)

# Índices por bucket
BUCKET_INDICES = {
    "RF":          [0, 1, 2, 3, 4, 5, 6, 7],
    "Alternativos": [8, 9, 10],
    "RV":          [11, 12, 13, 14],
}

BUCKET_LABELS = {
    "RF":           "Renda Fixa",
    "Alternativos": "Alternativos",
    "RV":           "Renda Variável",
}

# Volatilidades históricas de referência (% a.a.)
DEFAULT_VOLS_PCT = {
    "RF Pré Curta (IRF-M 1)":       0.679,
    "RF Pré Longa (IRF-M 1+)":      4.989,
    "RF Inflação Curta (IMA-B 5)":   2.898,
    "RF Inflação Longa (IMA-B 5+)": 10.440,
    "RF Inflação Deb. GI":           4.403,
    "RF Inflação Deb. HY":          10.455,
    "RF Pós Crédito GI":             1.526,
    "RF Pós Crédito HY":             1.507,
    "Multimercado (IHFA)":           3.817,
    "Ouro":                         15.969,
    "FIIs (IFIX)":                   9.088,
    "RV Brasil (Ibovespa)":         21.538,
    "RV EUA c/ câmbio":             18.630,
    "RV EUA s/ câmbio":             16.195,
    "Bitcoin":                      70.000,
}

# Matriz de correlação sintética padrão (15×15)
def _default_corr() -> np.ndarray:
    PRC,PRL,INC,INL,INGI,INHY,POSGI,POSHY,IHFA,OURO,FII,IBOV,RVEC,RVES,BTC = range(15)
    C = np.eye(15)

    def sc(i, j, v):
        C[i, j] = v
        C[j, i] = v

    sc(PRC,PRL,0.75); sc(PRC,INC,0.35); sc(PRC,INL,0.25); sc(PRC,INGI,0.25)
    sc(PRC,INHY,0.20); sc(PRC,POSGI,0.10); sc(PRC,POSHY,0.10); sc(PRC,IHFA,0.20)
    sc(PRC,OURO,0.05); sc(PRC,FII,0.15); sc(PRC,IBOV,0.10); sc(PRC,RVEC,0.05)
    sc(PRC,RVES,0.05); sc(PRC,BTC,0.02)
    sc(PRL,INC,0.45); sc(PRL,INL,0.55); sc(PRL,INGI,0.40); sc(PRL,INHY,0.30)
    sc(PRL,POSGI,0.15); sc(PRL,POSHY,0.15); sc(PRL,IHFA,0.30); sc(PRL,OURO,0.05)
    sc(PRL,FII,0.25); sc(PRL,IBOV,0.15); sc(PRL,RVEC,0.05); sc(PRL,RVES,0.05); sc(PRL,BTC,0.02)
    sc(INC,INL,0.70); sc(INC,INGI,0.65); sc(INC,INHY,0.50); sc(INC,POSGI,0.20)
    sc(INC,POSHY,0.20); sc(INC,IHFA,0.25); sc(INC,OURO,0.15); sc(INC,FII,0.35)
    sc(INC,IBOV,0.15); sc(INC,RVEC,0.05); sc(INC,RVES,0.05); sc(INC,BTC,0.02)
    sc(INL,INGI,0.60); sc(INL,INHY,0.55); sc(INL,POSGI,0.15); sc(INL,POSHY,0.15)
    sc(INL,IHFA,0.35); sc(INL,OURO,0.20); sc(INL,FII,0.45); sc(INL,IBOV,0.25)
    sc(INL,RVEC,0.10); sc(INL,RVES,0.05); sc(INL,BTC,0.03)
    sc(INGI,INHY,0.70); sc(INGI,POSGI,0.30); sc(INGI,POSHY,0.25); sc(INGI,IHFA,0.30)
    sc(INGI,OURO,0.10); sc(INGI,FII,0.35); sc(INGI,IBOV,0.20); sc(INGI,RVEC,0.05)
    sc(INGI,RVES,0.05); sc(INGI,BTC,0.02)
    sc(INHY,POSGI,0.25); sc(INHY,POSHY,0.25); sc(INHY,IHFA,0.40); sc(INHY,OURO,0.10)
    sc(INHY,FII,0.45); sc(INHY,IBOV,0.35); sc(INHY,RVEC,0.10); sc(INHY,RVES,0.05); sc(INHY,BTC,0.03)
    sc(POSGI,POSHY,0.85); sc(POSGI,IHFA,0.20); sc(POSGI,OURO,0.05); sc(POSGI,FII,0.15)
    sc(POSGI,IBOV,0.10); sc(POSGI,RVEC,0.05); sc(POSGI,RVES,0.02); sc(POSGI,BTC,0.02)
    sc(POSHY,IHFA,0.20); sc(POSHY,OURO,0.05); sc(POSHY,FII,0.15); sc(POSHY,IBOV,0.10)
    sc(POSHY,RVEC,0.05); sc(POSHY,RVES,0.02); sc(POSHY,BTC,0.02)
    sc(IHFA,OURO,0.10); sc(IHFA,FII,0.40); sc(IHFA,IBOV,0.55); sc(IHFA,RVEC,0.30)
    sc(IHFA,RVES,0.25); sc(IHFA,BTC,0.10)
    sc(OURO,FII,0.10); sc(OURO,IBOV,0.05); sc(OURO,RVEC,-0.10); sc(OURO,RVES,0.10); sc(OURO,BTC,0.20)
    sc(FII,IBOV,0.60); sc(FII,RVEC,0.15); sc(FII,RVES,0.10); sc(FII,BTC,0.05)
    sc(IBOV,RVEC,0.45); sc(IBOV,RVES,0.30); sc(IBOV,BTC,0.15)
    sc(RVEC,RVES,0.75); sc(RVEC,BTC,0.15); sc(RVES,BTC,0.10)
    return C

DEFAULT_CORR = _default_corr()

# Correlações macro entre buckets (RF, Alt, RV)
DEFAULT_MACRO_CORR = np.array([
    [1.00, 0.25, 0.20],
    [0.25, 1.00, 0.45],
    [0.20, 0.45, 1.00],
])

# Limites máximos de peso por ativo (ex-caixa)
DEFAULT_MAX_WEIGHTS = np.array([
    0.50, 0.20, 0.30, 0.20, 0.20, 0.15, 0.40, 0.40,
    0.30, 0.15, 0.20,
    0.40, 0.30, 0.30, 0.12,
])

# Portfólio âncora conservador (perfil 1)
_W_P1_RAW = np.array([
    0.35, 0.02, 0.15, 0.02, 0.08, 0.03, 0.20, 0.10,
    0.02, 0.01, 0.01,
    0.00, 0.00, 0.00, 0.00,
])

# Portfólio âncora agressivo (perfil 10)
_W_P10_RAW = np.array([
    0.020, 0.030, 0.015, 0.060, 0.030, 0.020, 0.005, 0.005,
    0.100, 0.050, 0.045,
    0.230, 0.150, 0.150, 0.090,
])


# ── FUNÇÕES MATEMÁTICAS ───────────────────────────────────────────────────────

def _port_vol(w: np.ndarray, cov: np.ndarray) -> float:
    return float(np.sqrt(w @ cov @ w))


def _risk_contrib(w: np.ndarray, cov: np.ndarray) -> np.ndarray:
    pv = _port_vol(w, cov)
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


def _build_block_diag_corr(
    n_assets: int,
    bucket_indices: dict[str, list[int]],
    intra_corrs: dict[str, np.ndarray],
) -> np.ndarray:
    """
    Constrói uma matriz de correlação completa bloco-diagonal:
    correlações intra-bucket fornecidas; correlação zero entre buckets.
    Usada quando corr_matrix global não é fornecida para classes customizadas.
    """
    corr = np.eye(n_assets)
    for bkey, idxs in bucket_indices.items():
        c = intra_corrs[bkey]
        for i, ii in enumerate(idxs):
            for j, jj in enumerate(idxs):
                corr[ii, jj] = c[i, j]
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
) -> tuple[np.ndarray, np.ndarray]:
    """
    Retorna (w_hrp, w_macro) onde:
      w_hrp   → pesos de todos os ativos no portfólio HRP
      w_macro → pesos dos buckets no nível macro
    """
    _bkt_idxs = bucket_indices if bucket_indices is not None else BUCKET_INDICES
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

        bnds = [(0.01, 0.60)] * len(idxs) if bkey == "RV" else None
        w_bkt = _solve_rp(len(idxs), COV_bkt, bnds)
        w_bkts[bkey] = w_bkt
        # Vol realizada do portfólio RP intra-bucket como representante do bucket
        bvols[k_idx] = _port_vol(w_bkt, COV_bkt)

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


def _default_intra_corrs() -> dict[str, np.ndarray]:
    """Matrizes de correlação intra-bucket padrão."""
    C_rf = np.array([
        [1.00, 0.75, 0.35, 0.25, 0.25, 0.20, 0.10, 0.10],
        [0.75, 1.00, 0.45, 0.55, 0.40, 0.30, 0.15, 0.15],
        [0.35, 0.45, 1.00, 0.70, 0.65, 0.50, 0.20, 0.20],
        [0.25, 0.55, 0.70, 1.00, 0.60, 0.55, 0.15, 0.15],
        [0.25, 0.40, 0.65, 0.60, 1.00, 0.70, 0.30, 0.25],
        [0.20, 0.30, 0.50, 0.55, 0.70, 1.00, 0.25, 0.25],
        [0.10, 0.15, 0.20, 0.15, 0.30, 0.25, 1.00, 0.85],
        [0.10, 0.15, 0.20, 0.15, 0.25, 0.25, 0.85, 1.00],
    ])
    C_alt = np.array([
        [1.00, 0.10, 0.40],
        [0.10, 1.00, 0.10],
        [0.40, 0.10, 1.00],
    ])
    C_rv = np.array([
        [1.00, 0.45, 0.30, 0.15],
        [0.45, 1.00, 0.75, 0.15],
        [0.30, 0.75, 1.00, 0.10],
        [0.15, 0.15, 0.10, 1.00],
    ])
    return {"RF": C_rf, "Alternativos": C_alt, "RV": C_rv}


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
) -> np.ndarray:
    """
    Para um vol_target dado, encontra o portfólio mais próximo ao HRP
    que atinge exatamente aquela volatilidade.
    """
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
        return float(np.sum((w - w_ref) ** 2 * (1 + 5 * w_ref)))

    constraints = [
        {"type": "eq", "fun": lambda w: w.sum() - 1},
        {"type": "eq", "fun": lambda w: _port_vol(w, cov) - vol_target},
    ]
    bounds = [(0.0, float(max_weights[i])) for i in range(len(max_weights))]

    best_w, best_obj = None, np.inf
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
        except Exception as exc:
            warnings.warn(
                f"_optimize_profile: falha na otimização com ponto inicial — {exc}",
                RuntimeWarning,
                stacklevel=3,
            )

    if best_w is None:
        best_w = w_ref.copy()

    best_w = np.clip(best_w, 0.0, 1.0)
    best_w /= best_w.sum()

    # Zerar posições abaixo do threshold mínimo
    best_w[best_w < min_weight_threshold] = 0.0
    if best_w.sum() > 0:
        best_w /= best_w.sum()

    return best_w


# ── FUNÇÃO PRINCIPAL ──────────────────────────────────────────────────────────

def build_spectrum(
    vols_pct: dict[str, float] | None = None,
    corr_matrix: np.ndarray | None = None,
    macro_corr: np.ndarray | None = None,
    intra_corrs: dict[str, np.ndarray] | None = None,
    sigma_min_pct: float = 1.0,
    sigma_max_pct: float = 11.92,
    max_weights: np.ndarray | None = None,
    min_weight_threshold: float = 0.005,
    n_profiles: int = 10,
    classes: list[str] | None = None,
    bucket_indices: dict[str, list[int]] | None = None,
    bucket_labels: dict[str, str] | None = None,
    w_anchor_conservative: np.ndarray | None = None,
    w_anchor_aggressive: np.ndarray | None = None,
) -> dict:
    """
    Calcula o espectro completo de alocação.

    Parâmetros
    ----------
    vols_pct : dict {classe: vol em % a.a.}
        Volatilidades anualizadas. Se None, usa os defaults (15 classes).
    corr_matrix : np.ndarray (n×n)
        Matriz de correlação global. Se None e usando classes default, usa a
        sintética padrão. Se None e usando classes customizadas, constrói
        bloco-diagonal a partir de intra_corrs (ou identidade, com aviso).
    macro_corr : np.ndarray (k×k, k = nº de buckets)
        Correlações entre buckets. Se None, usa o default (3×3 para 3 buckets)
        ou identidade para outros casos.
    intra_corrs : dict {bucket: np.ndarray}
        Correlações intra-bucket. Se None, usa os defaults quando possível.
    sigma_min_pct : float
        Volatilidade-alvo do perfil 1 (% a.a.). Default: 1.0
    sigma_max_pct : float
        Volatilidade-alvo do perfil 10 (% a.a.). Default: 11.92
    max_weights : np.ndarray (n,)
        Limite máximo de peso por ativo. Se None, usa os defaults para as 15
        classes padrão, ou 1.0 para cada ativo em universos customizados.
    min_weight_threshold : float
        Pesos abaixo deste valor são zerados. Default: 0.005 (0,5%)
    n_profiles : int
        Número de perfis. Default: 10.
    classes : list[str]
        Nomes das classes de ativos. Se None, usa o universo padrão de 15 classes.
    bucket_indices : dict {bucket: list[int]}
        Mapeamento bucket → índices dos ativos em `classes`. Se None, usa o
        padrão {RF: [0-7], Alternativos: [8-10], RV: [11-14]}.
    bucket_labels : dict {bucket: str}
        Rótulos descritivos dos buckets. Se None, usa os defaults.
    w_anchor_conservative : np.ndarray (n,)
        Portfólio-âncora para interpolar o perfil mais conservador.
        Se None, usa o padrão (pesos do perfil 1 histórico) para as 15 classes,
        ou pesos iguais para universos customizados.
    w_anchor_aggressive : np.ndarray (n,)
        Portfólio-âncora para interpolar o perfil mais agressivo.
        Se None, usa o padrão (pesos do perfil 10 histórico) para as 15 classes,
        ou pesos iguais para universos customizados.

    Retorna
    -------
    dict com chaves:
      classes        → list[str]         nomes das classes
      vols           → np.ndarray        vols em decimal
      cov            → np.ndarray        matriz de covariância (n×n)
      vol_targets    → np.ndarray        vols-alvo em decimal (n_profiles,)
      w_hrp          → np.ndarray        pesos HRP (n,)
      w_macro        → np.ndarray        pesos macro por bucket (k,)
      vol_hrp        → float             vol do portfólio HRP (%)
      weights        → dict[int→array]   pesos por perfil
      vol_realized   → dict[int→float]   vol realizada por perfil (%)
      risk_contrib   → dict[int→array]   contrib. de risco por perfil (fração)
      bucket_indices → dict              mapeamento bucket → índices
      bucket_labels  → dict              rótulos dos buckets
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
        if not np.allclose(corr_matrix, corr_matrix.T, atol=1e-8):
            raise ValueError("corr_matrix não é simétrica")
        min_eig = float(np.linalg.eigvalsh(corr_matrix).min())
        if min_eig < -1e-8:
            raise ValueError(
                f"corr_matrix não é positiva semi-definida (menor autovalor: {min_eig:.6f})"
            )

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
    if corr_matrix is not None:
        cov = _build_cov(vols, corr_matrix)
        _intra = {bkey: corr_matrix[np.ix_(idxs, idxs)] for bkey, idxs in _bucket_indices.items()}
        _macro = macro_corr if macro_corr is not None else (
            DEFAULT_MACRO_CORR if n_buckets == 3 else np.eye(n_buckets)
        )
    elif _using_default_universe and intra_corrs is None:
        cov = _build_cov(vols, DEFAULT_CORR)
        _intra = _default_intra_corrs()
        _macro = macro_corr if macro_corr is not None else DEFAULT_MACRO_CORR
    else:
        # Classes customizadas ou intra_corrs explícitas: constrói bloco-diagonal
        _intra = intra_corrs if intra_corrs is not None else {
            bkey: np.eye(len(idxs)) for bkey, idxs in _bucket_indices.items()
        }
        if intra_corrs is None:
            warnings.warn(
                "intra_corrs não fornecida para classes customizadas — usando correlação zero entre ativos.",
                UserWarning,
                stacklevel=2,
            )
        block_corr = _build_block_diag_corr(n_assets, _bucket_indices, _intra)
        cov = _build_cov(vols, block_corr)
        _macro = macro_corr if macro_corr is not None else (
            DEFAULT_MACRO_CORR if n_buckets == 3 else np.eye(n_buckets)
        )

    # ── HRP ───────────────────────────────────────────────────────────────────
    w_hrp, w_macro = _compute_hrp(vols, _macro, _intra, _bucket_indices)
    vol_hrp = _port_vol(w_hrp, cov) * 100.0

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

    vol_p10 = _port_vol(w_p10, cov) * 100.0

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

    for p in range(1, n_profiles + 1):
        vt = float(vol_targets[p - 1])
        w = _optimize_profile(
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
        )
        weights[p] = w
        vol_realized[p] = _port_vol(w, cov) * 100.0

        rc = _risk_contrib(w, cov)
        rc_total = rc.sum()
        risk_contrib[p] = rc / rc_total if rc_total > 0 else rc

    return {
        "classes":        _classes,
        "vols":           vols,
        "cov":            cov,
        "vol_targets":    vol_targets,
        "w_hrp":          w_hrp,
        "w_macro":        w_macro,
        "vol_hrp":        vol_hrp,
        "weights":        weights,
        "vol_realized":   vol_realized,
        "risk_contrib":   risk_contrib,
        "bucket_indices": _bucket_indices,
        "bucket_labels":  _bucket_labels,
    }


def get_profile_summary(result: dict, profile: int) -> dict:
    """
    Retorna um resumo legível de um perfil específico.

    Exemplo:
        summary = get_profile_summary(result, profile=5)
        summary['bucket_weights']   → {'RF': 0.62, 'Alternativos': 0.27, 'RV': 0.11}
        summary['asset_weights']    → [{'class': ..., 'weight': ..., 'risk_contrib': ...}]
        summary['vol_target']       → 3.01  (%)
        summary['vol_realized']     → 3.01  (%)
    """
    w = result["weights"][profile]
    rc = result["risk_contrib"][profile]
    vt = result["vol_targets"][profile - 1] * 100.0
    vr = result["vol_realized"][profile]

    bucket_weights = {
        bkey: float(sum(w[i] for i in idxs))
        for bkey, idxs in result["bucket_indices"].items()
    }

    asset_weights = [
        {
            "class":       result["classes"][i],
            "bucket":      next(k for k, v in result["bucket_indices"].items() if i in v),
            "weight":      float(w[i]),
            "risk_contrib": float(rc[i]),
            "vol_hist":    float(result["vols"][i] * 100.0),
        }
        for i in range(len(result["classes"]))
        if w[i] >= 0.001
    ]

    return {
        "profile":        profile,
        "vol_target":     vt,
        "vol_realized":   vr,
        "bucket_weights": bucket_weights,
        "asset_weights":  asset_weights,
    }

result = build_spectrum(
    n_profiles=100,
    classes=["NTN-B 2035", "Ibovespa", "S&P 500"],
    bucket_indices={"RF": [0], "RV": [1, 2]},
    bucket_labels={"RF": "Renda Fixa", "RV": "Renda Variável"},
    vols_pct={"NTN-B 2035": 8.5, "Ibovespa": 22.0, "S&P 500": 18.0},
    intra_corrs={
        "RF": np.array([[1.0]]),
        "RV": np.array([[1.0, 0.45], [0.45, 1.0]]),
    },
    macro_corr=np.array([[1.0, 0.15], [0.15, 1.0]]),
    max_weights=np.array([0.80, 0.50, 0.50]),
    w_anchor_conservative=np.array([0.90, 0.05, 0.05]),
    w_anchor_aggressive=np.array([0.10, 0.50, 0.40]),
)

summary = get_profile_summary(result, profile=90)
print(summary)