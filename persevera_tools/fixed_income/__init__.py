from .data import get_emissions, get_series, get_references
from .metrics import calculate_spread, calculate_duration
from .pipeline import SPREAD_SERIES, run_spread_pipeline

__all__ = [
    'get_emissions',
    'get_series',
    'get_references',
    'calculate_spread',
    'calculate_duration',
    'SPREAD_SERIES',
    'run_spread_pipeline',
]
