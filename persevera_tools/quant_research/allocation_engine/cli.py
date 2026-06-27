"""
cli.py
======
Interface de linha de comando para o motor de alocação.

Uso:
    python -m persevera_tools.quant_research.allocation_engine --help
"""

from __future__ import annotations

import argparse

from .core import AllocationConfig, AllocationEngine, Asset
from .loaders import load_snapshot


def _parse_asset(s: str) -> Asset:
    """
    Parse asset string do CLI.
    Discreto  : "CRA022008NF:18:1059.38"       → ticker:total_units:unit_price
    Contínuo  : "M8CREDIT::59850.30"            → ticker::total_value
    """
    parts = s.split(":")
    if len(parts) != 3:
        raise ValueError(
            f"Formato inválido para ativo '{s}'. "
            "Use TICKER:QTD:PRECO (discreto) ou TICKER::VALOR_TOTAL (contínuo)."
        )
    ticker = parts[0]
    if not ticker:
        raise ValueError(f"Ticker vazio na string de ativo '{s}'.")
    try:
        if parts[1] == "":
            return Asset(ticker=ticker, total_value=float(parts[2]))
        return Asset(ticker=ticker, total_units=int(parts[1]), unit_price=float(parts[2]))
    except (ValueError, TypeError) as exc:
        raise ValueError(
            f"Valores numéricos inválidos no ativo '{s}': {exc}"
        ) from exc


def main():
    parser = argparse.ArgumentParser(
        description="Motor de alocação de ativos — agnóstico a instrumento"
    )
    parser.add_argument("snapshot", help="Caminho para o JSON de snapshot de portfólios")
    parser.add_argument(
        "--ativos", nargs="+", required=True,
        help=(
            "Lista de ativos. "
            "Discreto: TICKER:QTD:PRECO (ex: CRA022008NF:18:1059.38). "
            "Contínuo: TICKER::VALOR_TOTAL (ex: MEUFI::100000)"
        ),
    )
    parser.add_argument("--officer", help="Filtrar por officer (ex: 'Otavio Ferreira' ou 'otavio')")
    parser.add_argument("--exclude", nargs="+", metavar="COD",
                        help="Codinomes a ignorar (ex: --exclude GAGU FETT CLAC)")
    parser.add_argument("--min-pct", type=float, default=1.5,
                        help="Alocação mínima por ativo (%% PL). Default: 1.5")
    parser.add_argument("--max-pct", type=float, default=2.0,
                        help="Alocação máxima por ativo (%% PL). Default: 2.0")
    parser.add_argument("--min-cash", type=float, default=5.0,
                        help="Caixa mínimo pós-alocação (%% PL). Default: 5.0")
    parser.add_argument("--objetivo", choices=["max_clients", "max_volume"],
                        default="max_clients")
    parser.add_argument("--ignore-existing", action="store_true",
                        help="Ignorar posições existentes no cálculo do gap")
    parser.add_argument("--no-topup", action="store_true",
                        help="Não distribuir cotas remanescentes")
    args = parser.parse_args()

    clients = load_snapshot(
        args.snapshot,
        officer_filter=args.officer,
        exclude=args.exclude,
    )

    assets = [_parse_asset(s) for s in args.ativos]

    config = AllocationConfig(
        min_pct=args.min_pct / 100,
        max_pct=args.max_pct / 100,
        min_cash_pct_after=args.min_cash / 100,
        objective=args.objetivo,
        consider_existing=not args.ignore_existing,
        topup=not args.no_topup,
    )

    engine = AllocationEngine(config)
    result = engine.allocate(clients, assets)
    print(result.summary())


if __name__ == "__main__":
    main()
