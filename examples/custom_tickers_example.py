from persevera_tools.data import FinancialDataService
from persevera_tools.db.operations import read_sql

fds = FinancialDataService(start_date='2025-03-01')

# sgs_data = fds.get_data(
#     source='sgs',
#     save_to_db=True
# )

simplify_data = fds.get_data(
    source='simplify',
    save_to_db=True,
)

print(simplify_data)

