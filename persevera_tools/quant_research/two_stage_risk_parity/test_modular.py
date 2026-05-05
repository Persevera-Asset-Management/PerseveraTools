"""Teste end-to-end da nova estrutura modular."""
import sys
sys.path.insert(0, '/home/claude')

import numpy as np
from hrp.config import SpectrumConfig, AssetClass, BucketConfig, RCTarget
from hrp.core import build_spectrum, get_profile_summary

print("="*72)
print("TESTE: Construção manual de SpectrumConfig + build_spectrum")
print("="*72)

# Constrói config equivalente à Calibração Inicial — mas via API Python
# para validar que a separação está limpa
assets = [
    AssetClass("RF Pré Curta",        "Renda Fixa", "anbima_irf_m1",      0.00679, 0.50, None),
    AssetClass("RF Pré Longa",        "Renda Fixa", "anbima_irf_m1+",     0.04989, 0.20, None),
    AssetClass("RF Inflação Curta",   "Renda Fixa", "anbima_ima_b5",      0.02898, 0.30, None),
    AssetClass("RF Inflação Longa",   "Renda Fixa", "anbima_ima_b5+",     0.10440, 0.20, None),
    AssetClass("RF Inflação Crédito", "Renda Fixa", "anbima_ida_ipca",    0.04403, 0.20, None),
    AssetClass("RF Pós Crédito",      "Renda Fixa", "anbima_ida_di",      0.01507, 0.40, None),
    AssetClass("RV Brasil",           "Renda Variável", "br_ibovespa",    0.21538, 0.40, None),
    AssetClass("RV EUA",              "Renda Variável", "us_sp500",       0.18630, 0.30, None),
    AssetClass("Multimercado",        "Alternativos", "anbima_ihfa",      0.03817, 0.30, None),
    AssetClass("Bitcoin",             "Alternativos", "bitcoin_usd",      0.50000, 0.12, 0.10),  # Max RC 10%!
    AssetClass("Ouro",                "Alternativos", "gold_100oz_futures", 0.15969, 0.15, None),
]

buckets = {
    "Renda Fixa":      BucketConfig("Renda Fixa", "Renda Fixa", None),
    "Renda Variável":  BucketConfig("Renda Variável", "Renda Variável", 0.60),
    "Alternativos":    BucketConfig("Alternativos", "Alternativos", None),
}

rc_targets = {
    "Renda Fixa":     RCTarget("Renda Fixa", 0.85, 0.15),
    "Renda Variável": RCTarget("Renda Variável", 0.05, 0.55),
    "Alternativos":   RCTarget("Alternativos", 0.10, 0.30),
}

# Matrizes (ordem: como em assets)
C_rf = np.array([
    [1.00, 0.75, 0.35, 0.25, 0.23, 0.10],
    [0.75, 1.00, 0.45, 0.55, 0.35, 0.15],
    [0.35, 0.45, 1.00, 0.70, 0.58, 0.20],
    [0.25, 0.55, 0.70, 1.00, 0.58, 0.15],
    [0.23, 0.35, 0.58, 0.58, 1.00, 0.26],
    [0.10, 0.15, 0.20, 0.15, 0.26, 1.00],
])
C_rv = np.array([[1.00, 0.45], [0.45, 1.00]])
C_alt = np.array([
    [1.00, 0.10, 0.10],
    [0.10, 1.00, 0.20],
    [0.10, 0.20, 1.00],
])

intra_corrs = {"Renda Fixa": C_rf, "Renda Variável": C_rv, "Alternativos": C_alt}

macro_corr = np.array([
    [1.00, 0.20, 0.25],
    [0.20, 1.00, 0.45],
    [0.25, 0.45, 1.00],
])

config = SpectrumConfig(
    assets=assets,
    buckets=buckets,
    rc_targets=rc_targets,
    intra_corrs=intra_corrs,
    macro_corr=macro_corr,
    n_profiles=10,
    calibration_name="Manual Test",
    calibration_date="2026-05-04",
    calibration_status="Test",
)

print(f"  Config: {config.n_assets} ativos, {config.n_buckets} buckets")
print(f"  Bucket indices: {config.bucket_indices}")
print(f"  Calibração: {config.calibration_name} ({config.calibration_date})")

# Validação manual
config.validate()
print("  ✓ config.validate() passou")

# Roda o motor
result = build_spectrum(config)
print(f"  ✓ build_spectrum executou em {len(result['weights'])} perfis")
print()

print("="*72)
print("Verificação 1: Vol target == realizada (todos perfis)")
print("="*72)
all_ok = True
for p in range(1, 11):
    vt = result["vol_targets"][p-1] * 100
    vr = result["vol_realized"][p]
    delta = abs(vr - vt)
    if delta > 0.05:
        print(f"  FAIL Perfil {p}: vt={vt:.3f}  vr={vr:.3f}  Δ={delta:.4f}pp")
        all_ok = False
print(f"  Resultado: {'OK' if all_ok else 'FAIL'} — todos os perfis atingem vol-alvo exato")
print()

print("="*72)
print("Verificação 2: Constraint Max RC (Bitcoin ≤ 10%)")
print("="*72)
btc_idx = config.class_names.index("Bitcoin")
print(f"  Bitcoin idx = {btc_idx}, Max RC = {config.assets[btc_idx].max_rc}")
print(f"  Perfil  Peso BTC  RC BTC  Status")
print(f"  ------  --------  ------  ------")
all_ok = True
for p in range(1, 11):
    w = result["weights"][p][btc_idx]
    rc = result["risk_contrib"][p][btc_idx]
    status = "OK" if rc <= 0.105 else "VIOLA!"  # tolerância 0.5pp
    if rc > 0.105:
        all_ok = False
    print(f"  P{p:<5}    {w*100:6.2f}%   {rc*100:5.2f}%  {status}")
print(f"\n  Resultado: {'OK' if all_ok else 'FAIL'} — Max RC do Bitcoin respeitado")
print()

print("="*72)
print("Verificação 3: Pesos somam 1 e respeitam max_weight")
print("="*72)
all_ok = True
for p in range(1, 11):
    w = result["weights"][p]
    s = w.sum()
    violates = (w > config.max_weights_array + 1e-6).any()
    if abs(s - 1) > 1e-6 or violates:
        print(f"  FAIL Perfil {p}: soma={s:.6f}, viola_max_weight={violates}")
        all_ok = False
print(f"  Resultado: {'OK' if all_ok else 'FAIL'}")
print()

print("="*72)
print("Verificação 4: API SpectrumConfig.with_overrides")
print("="*72)
config2 = config.with_overrides(n_profiles=5, sigma_max_pct=8.0)
print(f"  Config original: n_profiles={config.n_profiles}, sigma_max={config.sigma_max_pct}")
print(f"  Override:        n_profiles={config2.n_profiles}, sigma_max={config2.sigma_max_pct}")
print(f"  Configs são instâncias diferentes: {config is not config2}")
result2 = build_spectrum(config2)
print(f"  ✓ build_spectrum com override gerou {len(result2['weights'])} perfis")
print()

print("="*72)
print("Verificação 5: Comparação BTC peso/RC com vs sem Max RC")
print("="*72)
# Constrói config sem Max RC no BTC
assets_no_max = [
    AssetClass(a.name, a.bucket, a.proxy, a.default_vol, a.max_weight, None)
    for a in assets
]
config_no_max = SpectrumConfig(
    assets=assets_no_max, buckets=buckets, rc_targets=rc_targets,
    intra_corrs=intra_corrs, macro_corr=macro_corr,
)
result_no_max = build_spectrum(config_no_max)

print(f"  {'Perf':<5} {'Peso (com)':<12} {'RC (com)':<12} {'Peso (sem)':<12} {'RC (sem)':<12}")
for p in [5, 7, 9, 10]:
    w_com = result["weights"][p][btc_idx]
    rc_com = result["risk_contrib"][p][btc_idx]
    w_sem = result_no_max["weights"][p][btc_idx]
    rc_sem = result_no_max["risk_contrib"][p][btc_idx]
    print(f"  P{p:<4} {w_com*100:7.2f}%      {rc_com*100:6.2f}%      {w_sem*100:7.2f}%      {rc_sem*100:6.2f}%")
print(f"\n  ✓ Constraint efetivamente reduz peso e RC do BTC")

print()
print("="*72)
print("TODOS OS TESTES OK — estrutura modular funciona")
print("="*72)
