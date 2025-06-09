from persevera_tools.data import FinancialDataService

fds = FinancialDataService()

sgs_data = fds.get_data(
    source='sgs',
    save_to_db=True
)

simplify_data = fds.get_data(
    source='simplify',
    save_to_db=False,
)
