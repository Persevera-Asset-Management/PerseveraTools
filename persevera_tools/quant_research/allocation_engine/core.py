"""
core.py
=======
Dataclasses e motor de alocação de ativos agnóstico a instrumento.

Suporta:
- Ativos discretos: CRA, CRI, Debêntures (alocação em cotas inteiras)
- Ativos contínuos: Fundos, ETFs (alocação em R$ livre)

Restrições configuráveis:
- Alocação por cliente entre [min_pct, max_pct] do PL
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

    def __post_init__(self):
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

    officer: nome do assessor/officer (opcional, para filtros externos).
    """
    code: str
    pl: float
    cash: float
    existing_positions: dict[str, float] = field(default_factory=dict)
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


@dataclass
class AllocationResult:
    """Resultado completo da rodada de alocação."""
    allocations: list[ClientAllocation]
    unique_clients: int
    total_value_allocated: float
    per_asset: dict[str, dict]   # {ticker: {allocated, remaining, n_clients, is_discrete}}
    config: AllocationConfig

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
        return self._build_result(clients, assets, raw)

    # ------------------------------------------------------------------
    # Eligibility
    # ------------------------------------------------------------------

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

                if target_max_val <= 0:
                    # Já está no target ou acima — não aloca mais
                    result[(client.code, asset.ticker)] = {"eligible": False,
                        "reason": "posição existente já >= max_pct"}
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
                total_primary = 0.0
                valid = True

                for i in subset_idx:
                    cod, min_u = primary_candidates[i]
                    total_primary += min_u
                    if total_primary > (primary.total_units if primary.is_discrete
                                        else primary.total_value):
                        valid = False
                        break
                    primary_alloc[cod] = min_u

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
                    )
                    used = 0.0
                    cap = sec_asset.total_units if sec_asset.is_discrete else sec_asset.total_value
                    for cod, min_u in sec_candidates:
                        val = sec_asset.value_for(min_u)
                        if used + (min_u if sec_asset.is_discrete else val) > cap + 1e-9:
                            continue
                        if cash_residual.get(cod, 0) < val + self.config.min_cash_pct_after * pl_map[cod]:
                            continue
                        secondary_alloc[(cod, sec_asset.ticker)] = min_u
                        cash_residual[cod] -= val
                        used += min_u if sec_asset.is_discrete else val

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
        pl_map = {c.code: c.pl for c in clients}

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
                max_u = elig["max_units"]
                max_val = elig["max_val"]
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
                    cash_residual[cod] -= alloc_u * asset.unit_price
                    used += alloc_u
                else:
                    effective_max_v = min(max_val, avail)
                    remaining_cap = cap - used
                    alloc_v = min(effective_max_v, remaining_cap)
                    if alloc_v < elig["min_units"]:
                        continue
                    assignment[(cod, asset.ticker)] = alloc_v
                    cash_residual[cod] -= alloc_v
                    used += alloc_v

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

        # Cash residual pós-alocação inicial
        cash_res = {c.code: c.cash for c in clients}
        pl_map = {c.code: c.pl for c in clients}
        for (cod, ticker), units in assignment.items():
            cash_res[cod] -= asset_map[ticker].value_for(units)

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
    ) -> AllocationResult:
        cfg = self.config
        pl_map = {c.code: c.pl for c in clients}
        cash_map = {c.code: c.cash for c in clients}
        existing_map = {c.code: c.existing_positions for c in clients}
        asset_map = {a.ticker: a for a in assets}

        # Compute cumulative spend per client
        spend: dict[str, float] = {}
        for (cod, ticker), units in assignment.items():
            spend[cod] = spend.get(cod, 0.0) + asset_map[ticker].value_for(units)

        allocations = []
        for (cod, ticker), units in sorted(assignment.items()):
            asset = asset_map[ticker]
            pl = pl_map[cod]
            cash_before = cash_map[cod]
            val = asset.value_for(units)
            existing = existing_map[cod].get(ticker, 0.0) if cfg.consider_existing else 0.0
            total_exp = (existing + val) / pl if pl > 0 else 0.0
            cash_after = cash_before - spend[cod]
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
    ) -> list[tuple[str, float]]:
        """
        Retorna [(client_code, min_units_or_val)] ordenados por min_units asc.
        Aplica cash_overrides se fornecido (para constraint conjunta multi-ativo).
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

            result.append((client.code, min_u))

        return sorted(result, key=lambda x: x[1])
