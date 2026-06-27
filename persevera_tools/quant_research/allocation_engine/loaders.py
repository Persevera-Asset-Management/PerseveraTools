"""
loaders.py
==========
Carregamento de snapshots de portfólios no formato padrão Persevera.
"""

from __future__ import annotations

import json
from typing import Optional

from .core import Client


def load_snapshot(
    path: str,
    officer_filter: Optional[str] = None,
    exclude: Optional[list[str]] = None,
) -> list[Client]:
    """
    Carrega o snapshot JSON de portfólios no formato padrão Persevera.

    Lê o campo 'officer_atual' diretamente de cada registro do snapshot.

    officer_filter : se fornecido, retorna apenas clientes cujo officer_atual
                     contenha a string (case-insensitive).
                     Ex: "Otavio Ferreira" ou apenas "otavio".
    exclude        : lista de codinomes a ignorar.
                     Ex: ["GAGU", "FETT", "CLAC"]
    """
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    excluded = set(exclude or [])

    clients = []
    for cod, d in data.items():
        pl = d.get("patrimonio_brl", 0)
        if pl <= 0:
            continue

        if cod in excluded:
            continue

        officer = d.get("officer_atual")  # None se campo ausente

        if officer_filter:
            if officer is None:
                continue
            if officer_filter.lower() not in officer.lower():
                continue

        cash = (
            d.get("distribuicao_por_classe", {})
             .get("Caixa e Equivalentes", {})
             .get("saldo_brl", 0.0)
        )

        # Posições existentes por ticker
        existing: dict[str, float] = {}
        for posicoes in d.get("posicoes_por_classe", {}).values():
            for pos in posicoes:
                ticker = (
                    pos.get("nome")
                    or pos.get("ticker")
                    or pos.get("codigo")
                    or ""
                )
                if ticker:
                    existing[ticker] = existing.get(ticker, 0.0) + pos.get("saldo_brl", 0.0)

        clients.append(Client(
            code=cod,
            pl=pl,
            cash=cash,
            existing_positions=existing,
            officer=officer,
        ))

    return clients
