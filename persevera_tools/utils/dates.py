from datetime import datetime, timedelta, date
from typing import List, Union
import pandas as pd


def get_holidays() -> List[pd.Timestamp]:
    """Read and return ANBIMA holidays from database."""
    # Use lazy import to avoid circular dependency
    from ..db.operations import read_sql
    
    query = "SELECT * FROM feriados_anbima"
    df = read_sql(query, date_columns=['date'])
    return df['date'].tolist()

def excel_to_datetime(serial_date):
    """Convert Excel serial date to datetime."""
    try:
        base_date = datetime(1970, 1, 1)
        return base_date + timedelta(days=serial_date)
    except (TypeError, ValueError):
        return None

def subtract_business_days(
    current_date: Union[date, datetime], days_to_subtract: int
) -> Union[date, datetime]:
    """Subtrai um número de dias úteis de uma data.

    A função considera feriados (obtidos de `get_holidays`) e fins de semana.

    Args:
        current_date: A data inicial.
        days_to_subtract: O número de dias úteis para subtrair.

    Returns:
        A data resultante após a subtração dos dias úteis.
    """
    holidays = {h.date() for h in get_holidays()}

    new_date = current_date
    days_subtracted = 0

    while days_subtracted < days_to_subtract:
        new_date -= timedelta(days=1)

        check_date = new_date.date() if isinstance(new_date, datetime) else new_date

        if check_date.weekday() < 5 and check_date not in holidays:
            days_subtracted += 1

    return new_date