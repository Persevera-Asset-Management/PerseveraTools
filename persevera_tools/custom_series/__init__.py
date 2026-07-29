"""
Séries customizadas construídas internamente (não vindas de providers).

Cada módulo implementa um pipeline que deriva um indicador e, opcionalmente,
faz upsert em ``indicadores`` com código ``persevera_*``.
"""

from .anbima_indices import (
    INDEX_CODE,
    INDEX_FIELD,
    build_index,
    classify_and_filter_ls,
    fetch_composition,
    run_anbima_ihfa_ls_pipeline,
)

__all__ = [
    "INDEX_CODE",
    "INDEX_FIELD",
    "build_index",
    "classify_and_filter_ls",
    "fetch_composition",
    "run_anbima_ihfa_ls_pipeline",
]
