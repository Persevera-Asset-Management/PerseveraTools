"""
core.py
=======
Dataclasses e motor de alocação de ativos agnóstico a instrumento.

Suporta:
- Ativos discretos: CRA, CRI, Debêntures (alocação em cotas inteiras)
- Ativos contínuos: Fundos, ETFs (alocação em R$ livre)

Restrições configuráveis:
- Alocação por cliente entre [min_pct, max_pct] do PL
- Exposição máxima por emissor (max_issuer_pct), via concentracao_emissores_rf no snapshot
- Caixa mínimo pós-alocação (min_cash_pct_after)
- Posição existente (opcional): considera exposição atual no cálculo do gap

Objetivo configurável:
- 'max_clients': maximiza número de clientes únicos alocados
- 'max_volume': maximiza volume total alocado (dentro das restrições)
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from itertools import combinations
from typing import Optional


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize_issuer(issuer: Optional[str]) -> Optional[str]:
    """Normaliza nome de emissor para comparação consistente."""
    if not issuer:
        return None
    return " ".join(issuer.upper().split())


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Asset:
    """
    Representa um ativo a ser alocado.

    Para ativos discretos (CRA, CRI, Debênture):
        unit_price  = preço por cota/unidade
        total_units = número de cotas disponíveis
        total_value = calculado automaticamente

    Para ativos contínuos (Fundos, ETFs):
        unit_price  = None
        total_units = None
        total_value = valor total disponível em R$
    """
    ticker: str
    total_units: Optional[int] = None    # None → contínuo
    unit_price: Optional[float] = None   # None → contínuo
    total_value: Optional[float] = None  # obrigatório se contínuo
    issuer: Optional[str] = None         # emissor; chave compatível com o snapshot

    def __post_init__(self):
        if self.issuer is not None:
            self.issuer = normalize_issuer(self.issuer)
        if self.is_discrete:
            self.total_value = self.total_units * self.unit_price
        elif self.total_value is None:
            raise ValueError(f"Ativo contínuo '{self.ticker}' requer total_value.")

    @property
    def is_discrete(self) -> bool:
        return self.unit_price is not None and self.total_units is not None

    def value_for(self, units_or_value: float) -> float:
        """Converte cotas → R$ (discreto) ou passa o valor direto (contínuo)."""
        return units_or_value * self.unit_price if self.is_discrete else units_or_value

    def units_for(self, value: float) -> float:
        """Converte R$ → cotas (discreto) ou R$ (contínuo)."""
        return value / self.unit_price if self.is_discrete else value


@dataclass
class Client:
    """
    Representa um cliente/portfólio.

    existing_positions: dict {ticker: valor_atual_em_brl}
        Usado quando AllocationConfig.consider_existing = True
        para calcular o gap até o target.

    existing_by_issuer: dict {emissor: valor_atual_em_brl}
        Exposição agregada em RF por emissor (concentracao_emissores_rf).

    officer: nome do assessor/officer (opcional, para filtros externos).
    """
    code: str
    pl: float
    cash: float
    existing_positions: dict[str, float] = field(default_factory=dict)
    existing_by_issuer: dict[str, float] = field(default_factory=dict)
    officer: Optional[str] = None

    @property
    def cash_pct(self) -> float:
        return self.cash / self.pl if self.pl > 0 else 0.0


@dataclass
class AllocationConfig:
    """Parâmetros globais da alocação."""
    min_pct: float = 0.015            # exposição mínima por ativo (% PL)
    max_pct: float = 0.020            # exposição máxima por ativo (% PL)
    min_cash_pct_after: float = 0.05  # caixa mínimo pós-alocação (% PL)
    consider_existing: bool = True    # descontar posição existente do gap
    objective: str = "max_clients"    # 'max_clients' | 'max_volume'
    topup: bool = True                # distribuir cotas/valor remanescente
    topup_method: str = "proportional"  # 'proportional' | 'equal'
    max_issuer_pct: Optional[float] = None  # teto por emissor (% PL); None = desligado
    issuer_unknown_policy: str = "warn_allow"  # 'warn_allow' | 'block'

    def __post_init__(self):
        if not 0.0 <= self.min_pct <= 1.0:
            raise ValueError(f"min_pct deve estar em [0, 1], recebido: {self.min_pct}")
        if not 0.0 <= self.max_pct <= 1.0:
            raise ValueError(f"max_pct deve estar em [0, 1], recebido: {self.max_pct}")
        if self.min_pct > self.max_pct:
            raise ValueError(
                f"min_pct ({self.min_pct}) não pode ser maior que max_pct ({self.max_pct})"
            )
        if not 0.0 <= self.min_cash_pct_after <= 1.0:
            raise ValueError(
                f"min_cash_pct_after deve estar em [0, 1], recebido: {self.min_cash_pct_after}"
            )
        if self.objective not in ("max_clients", "max_volume"):
            raise ValueError(
                f"objective deve ser 'max_clients' ou 'max_volume', recebido: '{self.objective}'"
            )
        if self.topup_method not in ("proportional", "equal"):
            raise ValueError(
                f"topup_method deve ser 'proportional' ou 'equal', recebido: '{self.topup_method}'"
            )
        if self.max_issuer_pct is not None and not 0.0 <= self.max_issuer_pct <= 1.0:
            raise ValueError(
                f"max_issuer_pct deve estar em [0, 1], recebido: {self.max_issuer_pct}"
            )
        if self.issuer_unknown_policy not in ("warn_allow", "block"):
            raise ValueError(
                f"issuer_unknown_policy deve ser 'warn_allow' ou 'block', "
                f"recebido: '{self.issuer_unknown_policy}'"
            )


@dataclass
class ClientAllocation:
    """Resultado de alocação de um ativo para um cliente."""
    client_code: str
    ticker: str
    units: float          # cotas (discreto) ou R$ (contínuo)
    value: float          # R$
    pct_pl: float         # % do PL do cliente
    existing_value: float # posição existente em R$
    total_exposure: float # existing + novo, em % PL
    cash_before: float
    cash_after: float     # caixa após TODAS as alocações do cliente nesta rodada
    cash_pct_after: float
    issuer: Optional[str] = None
    issuer_exposure_before_pct: Optional[float] = None
    issuer_exposure_after_pct: Optional[float] = None
    binding_constraint: Optional[str] = None


@dataclass
class AllocationResult:
    """Resultado completo da rodada de alocação."""
    allocations: list[ClientAllocation]
    unique_clients: int
    total_value_allocated: float
    per_asset: dict[str, dict]   # {ticker: {allocated, remaining, n_clients, is_discrete}}
    config: AllocationConfig
    warnings: list[str] = field(default_factory=list)

    def to_dataframe(self):
        """Converte para pandas DataFrame (requer pandas instalado)."""
        import pandas as pd
        rows = []
        for a in self.allocations:
            rows.append({
                "Cliente": a.client_code,
                "Ativo": a.ticker,
                "Cotas/Valor": a.units,
                "Valor (R$)": round(a.value, 2),
                "% PL (nova)": round(a.pct_pl * 100, 2),
                "Exposição total %": round(a.total_exposure * 100, 2),
                "Caixa após (R$)": round(a.cash_after, 2),
                "Caixa após %": round(a.cash_pct_after * 100, 2),
            })
        return pd.DataFrame(rows)

    def summary(self) -> str:
        lines = [
            f"=== Resultado de Alocação ===",
            f"Clientes únicos alocados : {self.unique_clients}",
            f"Volume total alocado     : R${self.total_value_allocated:,.2f}",
            "",
        ]
        if self.warnings:
            lines.append("Avisos:")
            for warning in self.warnings:
                lines.append(f"  - {warning}")
            lines.append("")
        for ticker, info in self.per_asset.items():
            discrete = info.get("is_discrete", True)
            unit_label = "cotas" if discrete else "R$"
            lines.append(f"  {ticker}")
            val_str = f"R${info['allocated_value']:,.2f}" if info['allocated_units'] else '-'
            lines.append(f"    Alocado  : {info['allocated_units']} "
                         f"de {info['total_units']} {unit_label} ({val_str})")
            lines.append(f"    Restante : {info['remaining_units']} {unit_label}"
                         f" / R${info['remaining_value']:,.2f}")
            lines.append(f"    Clientes : {info['n_clients']}")
        lines.append("")
        col_label = "Cotas/R$"
        lines.append(f"{'Cliente':<8}  {'Ativo':<14}  {col_label:>8}  "
                     f"{'Valor R$':>12}  {'%PL':>6}  {'Cx após':>8}")
        lines.append("-" * 64)
        for a in sorted(self.allocations, key=lambda x: (x.ticker, x.client_code)):
            units_str = f"{int(a.units)}" if isinstance(a.units, float) and a.units == int(a.units) else f"{a.units:.2f}"
            lines.append(f"{a.client_code:<8}  {a.ticker:<14}  {units_str:>6}  "
                         f"R${a.value:>10,.2f}  {a.pct_pl*100:>5.2f}%  "
                         f"{a.cash_pct_after*100:>7.1f}%")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Core engine
# ---------------------------------------------------------------------------

class AllocationEngine:
    """
    Motor de alocação multi-ativo.

    Exemplo de uso:
        engine = AllocationEngine(config)
        result = engine.allocate(clients, assets)
        print(result.summary())
    """

    def __init__(self, config: Optional[AllocationConfig] = None):
        self.config = config or AllocationConfig()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def allocate(
        self,
        clients: list[Client],
        assets: list[Asset],
    ) -> AllocationResult:
        """
        Ponto de entrada principal.

        Retorna AllocationResult com todas as alocações e metadados.
        """
        cfg = self.config
        warnings = self._collect_warnings(assets)

        # 1. Para cada (cliente, ativo): calcular range válido de alocação
        eligibility = self._build_eligibility(clients, assets)

        # 2. Otimizar: atribuir ativos a clientes
        if cfg.objective == "max_clients":
            raw = self._optimize_max_clients(clients, assets, eligibility)
        else:
            raw = self._optimize_max_volume(clients, assets, eligibility)

        # 3. Top-up: distribuir cotas remanescentes dentro do max_pct
        if cfg.topup:
            raw = self._topup(clients, assets, raw, eligibility)

        # 4. Montar resultado
        return self._build_result(clients, assets, raw, warnings)

    # ------------------------------------------------------------------
    # Eligibility
    # ------------------------------------------------------------------

    def _collect_warnings(self, assets: list[Asset]) -> list[str]:
        warnings: list[str] = []
        if self.config.max_issuer_pct is None:
            return warnings
        for asset in assets:
            if not asset.issuer:
                if self.config.issuer_unknown_policy == "warn_allow":
                    warnings.append(
                        f"Ativo '{asset.ticker}' sem emissor definido; "
                        "limite por emissor ignorado para este ativo."
                    )
        return warnings

    def _issuer_headroom(
        self,
        client: Client,
        asset: Asset,
        issuer_allocated: Optional[dict[tuple[str, str], float]] = None,
    ) -> Optional[float]:
        """Folga em R$ até max_issuer_pct, ou None se limite desligado."""
        cfg = self.config
        if cfg.max_issuer_pct is None or not asset.issuer:
            return None
        pl = client.pl
        if pl <= 0:
            return 0.0
        round_val = 0.0
        if issuer_allocated is not None:
            round_val = issuer_allocated.get((client.code, asset.issuer), 0.0)
        existing = client.existing_by_issuer.get(asset.issuer, 0.0)
        return max(0.0, cfg.max_issuer_pct * pl - existing - round_val)

    def _apply_issuer_cap(
        self,
        client: Client,
        asset: Asset,
        max_val: float,
        max_u: float,
        issuer_allocated: Optional[dict[tuple[str, str], float]] = None,
    ) -> tuple[float, float]:
        headroom = self._issuer_headroom(client, asset, issuer_allocated)
        if headroom is None:
            return max_val, max_u
        capped_val = min(max_val, headroom)
        if asset.is_discrete:
            capped_u = math.floor(capped_val / asset.unit_price)
            return capped_u * asset.unit_price, float(capped_u)
        return capped_val, capped_val

    def _build_eligibility(
        self,
        clients: list[Client],
        assets: list[Asset],
    ) -> dict[tuple[str, str], dict]:
        """
        Retorna {(client_code, ticker): {min_units, max_units, min_val, max_val, eligible}}
        """
        cfg = self.config
        result = {}

        for client in clients:
            for asset in assets:
                pl = client.pl
                existing = client.existing_positions.get(asset.ticker, 0.0) \
                    if cfg.consider_existing else 0.0
                existing_pct = existing / pl if pl > 0 else 0.0

                # Gap = quanto ainda pode alocar até atingir min_pct (e até max_pct)
                target_min_val = max(0.0, cfg.min_pct * pl - existing)
                target_max_val = max(0.0, cfg.max_pct * pl - existing)

                if cfg.max_issuer_pct is not None:
                    if not asset.issuer:
                        if cfg.issuer_unknown_policy == "block":
                            result[(client.code, asset.ticker)] = {
                                "eligible": False,
                                "reason": "emissor desconhecido",
                            }
                            continue
                    else:
                        existing_issuer = client.existing_by_issuer.get(asset.issuer, 0.0)
                        target_max_issuer = max(
                            0.0, cfg.max_issuer_pct * pl - existing_issuer
                        )
                        target_max_val = min(target_max_val, target_max_issuer)

                if target_max_val <= 0:
                    reason = "posição existente já >= max_pct"
                    if cfg.max_issuer_pct is not None and asset.issuer:
                        existing_issuer = client.existing_by_issuer.get(asset.issuer, 0.0)
                        if existing_issuer >= cfg.max_issuer_pct * pl:
                            reason = "exposição ao emissor já >= max_issuer_pct"
                    result[(client.code, asset.ticker)] = {"eligible": False,
                        "reason": reason}
                    continue

                if asset.is_discrete:
                    min_u = math.ceil(target_min_val / asset.unit_price)
                    max_u = math.floor(target_max_val / asset.unit_price)
                    eligible = min_u >= 1 and min_u <= max_u
                    min_v = min_u * asset.unit_price
                    max_v = max_u * asset.unit_price
                else:
                    # Contínuo: qualquer valor no range é válido
                    min_u = target_min_val
                    max_u = target_max_val
                    min_v = min_u
                    max_v = max_u
                    eligible = min_v > 0

                # Verificar cash
                max_spend = client.cash - cfg.min_cash_pct_after * pl
                if max_spend < min_v:
                    result[(client.code, asset.ticker)] = {
                        "eligible": False,
                        "reason": f"caixa insuficiente (disp R${max_spend:,.0f} < mín R${min_v:,.0f})",
                        "min_units": min_u, "max_units": max_u,
                        "cash_available": max_spend,
                    }
                    continue

                # Limitar max pela disponibilidade de caixa
                if asset.is_discrete:
                    max_u_cash = math.floor(max_spend / asset.unit_price)
                    max_u = min(max_u, max_u_cash)
                    max_v = max_u * asset.unit_price
                else:
                    max_u = min(max_u, max_spend)
                    max_v = max_u

                result[(client.code, asset.ticker)] = {
                    "eligible": eligible,
                    "min_units": min_u,
                    "max_units": max_u,
                    "min_val": min_v,
                    "max_val": max_v,
                    "cash_available": max_spend,
                    "existing": existing,
                    "existing_pct": existing_pct,
                    "issuer": asset.issuer,
                }

        return result

    # ------------------------------------------------------------------
    # Optimization: max clients
    # ------------------------------------------------------------------

    def _optimize_max_clients(
        self,
        clients: list[Client],
        assets: list[Asset],
        eligibility: dict,
    ) -> dict[tuple[str, str], float]:
        """
        Maximiza clientes únicos.

        Estratégia:
        1. Para cada ativo, computar candidatos elegíveis ordenados por min_units asc.
        2. Enumerar subsets do ativo mais restrito (menor pool de unidades),
           e para cada subset verificar a atribuição greedy dos demais ativos.
        3. Selecionar a configuração com maior número de clientes únicos.
        4. Desempate: maior volume alocado.
        """
        # Ordenar ativos por total_units/total_value asc (mais restrito primeiro)
        sorted_assets = sorted(assets, key=lambda a: a.total_value or 0)
        pl_map = {c.code: c.pl for c in clients}
        asset_map = {a.ticker: a for a in assets}

        best_unique = -1
        best_volume = -1.0
        best_assignment: dict[tuple[str, str], float] = {}

        # Para o ativo mais restrito, enumerar subsets (até tamanho razoável)
        primary = sorted_assets[0]
        remaining_assets = sorted_assets[1:]

        primary_candidates = self._sorted_candidates(clients, primary, eligibility)

        # Limitar enumeração: subsets de tamanho até max_subset_size
        max_subset_size = min(len(primary_candidates), 6)  # evitar explosão combinatória

        for size in range(max_subset_size, -1, -1):
            if size == 0:
                # Tenta sem alocar o ativo primário
                candidate_subsets = [()]
            else:
                candidate_subsets = combinations(range(len(primary_candidates)), size)

            for subset_idx in candidate_subsets:
                # Construir alocação do ativo primário para este subset
                primary_alloc: dict[str, float] = {}
                issuer_allocated: dict[tuple[str, str], float] = {}
                total_primary = 0.0
                valid = True

                for i in subset_idx:
                    cod, min_u = primary_candidates[i]
                    client = next(c for c in clients if c.code == cod)
                    _, effective_min_u = self._apply_issuer_cap(
                        client, primary, primary.value_for(min_u), min_u, issuer_allocated
                    )
                    if effective_min_u < min_u:
                        valid = False
                        break
                    min_u = effective_min_u
                    total_primary += min_u
                    if total_primary > (primary.total_units if primary.is_discrete
                                        else primary.total_value):
                        valid = False
                        break
                    primary_alloc[cod] = min_u
                    if primary.issuer:
                        key = (cod, primary.issuer)
                        issuer_allocated[key] = (
                            issuer_allocated.get(key, 0.0) + primary.value_for(min_u)
                        )

                if not valid:
                    continue

                # Calcular cash residual para cada cliente após alocação primária
                cash_residual = {c.code: c.cash for c in clients}
                for cod, units in primary_alloc.items():
                    cash_residual[cod] -= primary.value_for(units)

                # Greedy para os demais ativos
                secondary_alloc: dict[tuple[str, str], float] = {}
                for sec_asset in remaining_assets:
                    sec_candidates = self._sorted_candidates(
                        clients, sec_asset, eligibility,
                        cash_overrides=cash_residual,
                        issuer_allocated=issuer_allocated,
                    )
                    used = 0.0
                    cap = sec_asset.total_units if sec_asset.is_discrete else sec_asset.total_value
                    for cod, min_u in sec_candidates:
                        client = next(c for c in clients if c.code == cod)
                        _, effective_min_u = self._apply_issuer_cap(
                            client, sec_asset, sec_asset.value_for(min_u), min_u,
                            issuer_allocated,
                        )
                        if effective_min_u < min_u:
                            continue
                        min_u = effective_min_u
                        val = sec_asset.value_for(min_u)
                        if used + (min_u if sec_asset.is_discrete else val) > cap + 1e-9:
                            continue
                        if cash_residual.get(cod, 0) < val + self.config.min_cash_pct_after * pl_map[cod]:
                            continue
                        secondary_alloc[(cod, sec_asset.ticker)] = min_u
                        cash_residual[cod] -= val
                        used += min_u if sec_asset.is_discrete else val
                        if sec_asset.issuer:
                            key = (cod, sec_asset.issuer)
                            issuer_allocated[key] = issuer_allocated.get(key, 0.0) + val

                # Combinar: único por cliente
                assignment: dict[tuple[str, str], float] = {}
                for cod, units in primary_alloc.items():
                    assignment[(cod, primary.ticker)] = units
                for (cod, ticker), units in secondary_alloc.items():
                    assignment[(cod, ticker)] = units

                unique = len(set(cod for cod, _ in assignment))
                volume = sum(
                    asset_map[t].value_for(u) for (_, t), u in assignment.items()
                )

                if (unique > best_unique) or (unique == best_unique and volume > best_volume):
                    best_unique = unique
                    best_volume = volume
                    best_assignment = assignment

            # Early exit: não há como superar o total de clientes disponíveis
            if best_unique >= len(clients):
                break

        return best_assignment

    def _optimize_max_volume(
        self,
        clients: list[Client],
        assets: list[Asset],
        eligibility: dict,
    ) -> dict[tuple[str, str], float]:
        """
        Maximiza volume alocado.
        Greedy simples: para cada ativo, aloca o máximo possível por cliente
        ordenado por max_val descending.
        """
        assignment: dict[tuple[str, str], float] = {}
        cash_residual = {c.code: c.cash for c in clients}
        issuer_allocated: dict[tuple[str, str], float] = {}
        pl_map = {c.code: c.pl for c in clients}
        client_map = {c.code: c for c in clients}

        for asset in assets:
            cap = asset.total_units if asset.is_discrete else asset.total_value
            used = 0.0
            candidates = sorted(
                [(c.code, eligibility.get((c.code, asset.ticker), {}))
                 for c in clients
                 if eligibility.get((c.code, asset.ticker), {}).get("eligible")],
                key=lambda x: -x[1].get("max_val", 0),
            )
            for cod, elig in candidates:
                client = client_map[cod]
                max_u = elig["max_units"]
                max_val = elig["max_val"]
                max_val, max_u = self._apply_issuer_cap(
                    client, asset, max_val, max_u, issuer_allocated
                )
                # Re-check cash with current residual
                avail = cash_residual[cod] - self.config.min_cash_pct_after * pl_map[cod]
                if asset.is_discrete:
                    max_u_cash = math.floor(avail / asset.unit_price)
                    effective_max_u = min(max_u, max_u_cash)
                    if effective_max_u < elig["min_units"]:
                        continue
                    remaining_cap = cap - used
                    alloc_u = min(effective_max_u, math.floor(remaining_cap))
                    if alloc_u < elig["min_units"]:
                        continue
                    assignment[(cod, asset.ticker)] = alloc_u
                    val = alloc_u * asset.unit_price
                    cash_residual[cod] -= val
                    used += alloc_u
                    if asset.issuer:
                        key = (cod, asset.issuer)
                        issuer_allocated[key] = issuer_allocated.get(key, 0.0) + val
                else:
                    effective_max_v = min(max_val, avail)
                    remaining_cap = cap - used
                    alloc_v = min(effective_max_v, remaining_cap)
                    if alloc_v < elig["min_units"]:
                        continue
                    assignment[(cod, asset.ticker)] = alloc_v
                    cash_residual[cod] -= alloc_v
                    used += alloc_v
                    if asset.issuer:
                        key = (cod, asset.issuer)
                        issuer_allocated[key] = issuer_allocated.get(key, 0.0) + alloc_v

        return assignment

    # ------------------------------------------------------------------
    # Top-up
    # ------------------------------------------------------------------

    def _topup(
        self,
        clients: list[Client],
        assets: list[Asset],
        assignment: dict[tuple[str, str], float],
        eligibility: dict,
    ) -> dict[tuple[str, str], float]:
        """
        Distribui cotas/valor remanescentes dentro do teto de max_pct.
        Mantém o número de clientes (não adiciona novos no top-up).
        """
        cfg = self.config
        assignment = dict(assignment)
        asset_map = {a.ticker: a for a in assets}
        client_map = {c.code: c for c in clients}

        # Cash residual pós-alocação inicial
        cash_res = {c.code: c.cash for c in clients}
        issuer_allocated: dict[tuple[str, str], float] = {}
        pl_map = {c.code: c.pl for c in clients}
        for (cod, ticker), units in assignment.items():
            asset = asset_map[ticker]
            val = asset.value_for(units)
            cash_res[cod] -= val
            if asset.issuer:
                key = (cod, asset.issuer)
                issuer_allocated[key] = issuer_allocated.get(key, 0.0) + val

        for asset in assets:
            allocated_clients = [(cod, units) for (cod, t), units in assignment.items()
                                 if t == asset.ticker]
            if not allocated_clients:
                continue

            used = sum(u for _, u in allocated_clients)
            cap = asset.total_units if asset.is_discrete else asset.total_value
            leftover = cap - used

            if leftover <= 0:
                continue

            # Capacidade de absorção por cliente (max_units - current)
            absorption = {}
            for cod, cur_u in allocated_clients:
                elig = eligibility.get((cod, asset.ticker), {})
                max_u = elig.get("max_units", cur_u)
                max_val = elig.get("max_val", asset.value_for(cur_u))
                client = client_map[cod]
                max_val, max_u = self._apply_issuer_cap(
                    client, asset, max_val, max_u, issuer_allocated
                )
                avail_cash = cash_res[cod] - cfg.min_cash_pct_after * pl_map[cod]
                if asset.is_discrete:
                    max_u_cash = math.floor(avail_cash / asset.unit_price)
                    effective_max = min(max_u, max_u_cash)
                else:
                    effective_max = min(max_u, avail_cash)
                slack = max(0.0, effective_max - cur_u)
                if slack > 0:
                    absorption[cod] = slack

            if not absorption:
                continue

            # Distribuir leftover proporcionalmente ao PL (ou igualmente)
            for _ in range(100):
                if leftover < 1e-6:
                    break
                eligible_cods = list(absorption.keys())
                if not eligible_cods:
                    break

                if cfg.topup_method == "proportional":
                    total_pl = sum(pl_map[c] for c in eligible_cods)
                    weights = {c: pl_map[c] / total_pl for c in eligible_cods}
                else:
                    weights = {c: 1 / len(eligible_cods) for c in eligible_cods}

                distributed = 0.0
                for cod in eligible_cods:
                    share = leftover * weights[cod]
                    if asset.is_discrete:
                        add = min(math.floor(share), math.floor(absorption[cod]))
                    else:
                        add = min(share, absorption[cod])
                    if add <= 0:
                        continue
                    assignment[(cod, asset.ticker)] += add
                    cash_res[cod] -= asset.value_for(add)
                    if asset.issuer:
                        key = (cod, asset.issuer)
                        issuer_allocated[key] = issuer_allocated.get(key, 0.0) + asset.value_for(add)
                    absorption[cod] -= add
                    if absorption[cod] <= 1e-6:
                        del absorption[cod]
                    distributed += add
                    leftover -= add

                if distributed < 1e-9:
                    # Distribute remainder one unit at a time to largest PL
                    for cod in sorted(eligible_cods, key=lambda x: -pl_map[x]):
                        if leftover < (1 if asset.is_discrete else 1e-6):
                            break
                        add = 1 if asset.is_discrete else leftover
                        if absorption.get(cod, 0) >= add:
                            assignment[(cod, asset.ticker)] += add
                            cash_res[cod] -= asset.value_for(add)
                            if asset.issuer:
                                key = (cod, asset.issuer)
                                issuer_allocated[key] = (
                                    issuer_allocated.get(key, 0.0) + asset.value_for(add)
                                )
                            absorption[cod] -= add
                            leftover -= add
                    break

        return assignment

    # ------------------------------------------------------------------
    # Result builder
    # ------------------------------------------------------------------

    def _build_result(
        self,
        clients: list[Client],
        assets: list[Asset],
        assignment: dict[tuple[str, str], float],
        warnings: Optional[list[str]] = None,
    ) -> AllocationResult:
        cfg = self.config
        pl_map = {c.code: c.pl for c in clients}
        cash_map = {c.code: c.cash for c in clients}
        existing_map = {c.code: c.existing_positions for c in clients}
        issuer_map = {c.code: c.existing_by_issuer for c in clients}
        asset_map = {a.ticker: a for a in assets}

        # Compute cumulative spend per client
        spend: dict[str, float] = {}
        issuer_round: dict[tuple[str, str], float] = {}
        for (cod, ticker), units in assignment.items():
            asset = asset_map[ticker]
            val = asset.value_for(units)
            spend[cod] = spend.get(cod, 0.0) + val
            if asset.issuer:
                key = (cod, asset.issuer)
                issuer_round[key] = issuer_round.get(key, 0.0) + val

        allocations = []
        for (cod, ticker), units in sorted(assignment.items()):
            asset = asset_map[ticker]
            pl = pl_map[cod]
            cash_before = cash_map[cod]
            val = asset.value_for(units)
            existing = existing_map[cod].get(ticker, 0.0) if cfg.consider_existing else 0.0
            total_exp = (existing + val) / pl if pl > 0 else 0.0
            cash_after = cash_before - spend[cod]

            issuer_before_pct = None
            issuer_after_pct = None
            binding = None
            if cfg.max_issuer_pct is not None and asset.issuer:
                existing_issuer = issuer_map[cod].get(asset.issuer, 0.0)
                round_before = issuer_round.get((cod, asset.issuer), 0.0) - val
                issuer_before_pct = (existing_issuer + round_before) / pl if pl > 0 else 0.0
                issuer_after_pct = (existing_issuer + round_before + val) / pl if pl > 0 else 0.0
                asset_cap = cfg.max_pct * pl - existing
                issuer_cap = cfg.max_issuer_pct * pl - existing_issuer - round_before
                if issuer_cap <= asset_cap + 1e-6 and val > 0:
                    binding = "issuer"
                elif asset_cap <= issuer_cap + 1e-6 and val > 0:
                    binding = "asset"
                else:
                    binding = "cash" if val > 0 else None

            allocations.append(ClientAllocation(
                client_code=cod,
                ticker=ticker,
                units=units,
                value=val,
                pct_pl=val / pl if pl > 0 else 0.0,
                existing_value=existing,
                total_exposure=total_exp,
                cash_before=cash_before,
                cash_after=cash_after,
                cash_pct_after=cash_after / pl if pl > 0 else 0.0,
                issuer=asset.issuer,
                issuer_exposure_before_pct=issuer_before_pct,
                issuer_exposure_after_pct=issuer_after_pct,
                binding_constraint=binding,
            ))

        per_asset = {}
        for asset in assets:
            allocs = [(cod, u) for (cod, t), u in assignment.items() if t == asset.ticker]
            used = sum(u for _, u in allocs)
            cap = asset.total_units if asset.is_discrete else asset.total_value
            per_asset[asset.ticker] = {
                "allocated_units": used,
                "total_units": cap,
                "remaining_units": cap - used,
                "allocated_value": asset.value_for(used),
                "remaining_value": asset.value_for(cap - used),
                "n_clients": len(allocs),
                "is_discrete": asset.is_discrete,
            }

        return AllocationResult(
            allocations=allocations,
            unique_clients=len(set(cod for cod, _ in assignment)),
            total_value_allocated=sum(a.value for a in allocations),
            per_asset=per_asset,
            config=cfg,
            warnings=warnings or [],
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _sorted_candidates(
        self,
        clients: list[Client],
        asset: Asset,
        eligibility: dict,
        cash_overrides: Optional[dict[str, float]] = None,
        issuer_allocated: Optional[dict[tuple[str, str], float]] = None,
    ) -> list[tuple[str, float]]:
        """
        Retorna [(client_code, min_units_or_val)] ordenados por min_units asc.
        Aplica cash_overrides e issuer_allocated se fornecidos.
        """
        cfg = self.config
        result = []
        for client in clients:
            elig = eligibility.get((client.code, asset.ticker), {})
            if not elig.get("eligible"):
                continue
            min_u = elig["min_units"]
            min_v = elig["min_val"]

            if cash_overrides is not None:
                cash = cash_overrides.get(client.code, client.cash)
                avail = cash - cfg.min_cash_pct_after * client.pl
                if avail < min_v:
                    continue

            _, effective_min_u = self._apply_issuer_cap(
                client, asset, min_v, min_u, issuer_allocated
            )
            if effective_min_u < min_u:
                continue

            result.append((client.code, min_u))

        return sorted(result, key=lambda x: x[1])
