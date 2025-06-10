from persevera_tools.data import FinancialDataService

fds = FinancialDataService(start_date='2025-06-01')

fred_data = fds.get_data(
    source='fred',
    save_to_db=True
)

anbima_data = fds.get_data(
    source='anbima',
    save_to_db=True
)

simplify_data = fds.get_data(
    source='simplify',
    save_to_db=True,
)
