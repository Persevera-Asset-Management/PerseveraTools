"""
allocation_engine
=================
Motor de alocação de ativos agnóstico a instrumento.

Uso como módulo (Streamlit, notebooks, etc.):
    from persevera_tools.quant_research.allocation_engine import (
        AllocationEngine, Asset, Client, AllocationConfig
    )

Uso via CLI:
    python -m persevera_tools.quant_research.allocation_engine --help
"""

from .core import (
    AllocationConfig,
    AllocationEngine,
    AllocationResult,
    Asset,
    Client,
    ClientAllocation,
)
from .loaders import load_snapshot

__all__ = [
    "AllocationConfig",
    "AllocationEngine",
    "AllocationResult",
    "Asset",
    "Client",
    "ClientAllocation",
    "load_snapshot",
]
