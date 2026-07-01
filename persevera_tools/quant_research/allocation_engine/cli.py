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
    Discreto  : "CRA022008NF:18:1059.38" ou "CRA022008NF:18:1059.38:EMISSOR"
    Contínuo  : "M8CREDIT::59850.30" ou "M8CREDIT::59850.30:EMISSOR"
    """
    parts = s.split(":")
    issuer = None
    if len(parts) == 4:
        issuer = parts[3] or None
        parts = parts[:3]
    if len(parts) != 3:
        raise ValueError(
            f"Formato inválido para ativo '{s}'. "
            "Use TICKER:QTD:PRECO[:EMISSOR] (discreto) ou TICKER::VALOR_TOTAL[:EMISSOR] (contínuo)."
        )
    ticker = parts[0]
    if not ticker:
        raise ValueError(f"Ticker vazio na string de ativo '{s}'.")
    try:
        if parts[1] == "":
            return Asset(ticker=ticker, total_value=float(parts[2]), issuer=issuer)
        return Asset(
            ticker=ticker,
            total_units=int(parts[1]),
            unit_price=float(parts[2]),
            issuer=issuer,
        )
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
            "Discreto: TICKER:QTD:PRECO[:EMISSOR] (ex: CRA022008NF:18:1059.38:BANCO BMG). "
            "Contínuo: TICKER::VALOR_TOTAL[:EMISSOR] (ex: MEUFI::100000)"
        ),
    )
    parser.add_argument("--officer", help="Filtrar por officer (ex: 'Otavio Ferreira' ou 'otavio')")
    parser.add_argument("--exclude", nargs="+", metavar="COD",
                        help="Codinomes a ignorar (ex: --exclude GAGU FETT CLAC)")
    parser.add_argument("--min-pct", type=float, default=1.5,
                        help="Alocação mínima por ativo (%% PL). Default: 1.5")
    parser.add_argument("--max-pct", type=float, default=2.0,
                        help="Alocação máxima por ativo (%% PL). Default: 2.0")
    parser.add_argument("--max-issuer-pct", type=float, default=None,
                        help="Exposição máxima por emissor (%% PL). Ex: 5.0")
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
        max_issuer_pct=args.max_issuer_pct / 100 if args.max_issuer_pct is not None else None,
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
