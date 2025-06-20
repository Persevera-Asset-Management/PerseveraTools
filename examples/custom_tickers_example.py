from persevera_tools.data import FinancialDataService
from persevera_tools.data.funds import get_persevera_peers

fds = FinancialDataService(start_date='2025-01-01')

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

invesco_data = fds.get_data(
    source='invesco',
    save_to_db=True,
)

bcb_focus_data = fds.get_data(
    source='bcb_focus',
    save_to_db=False
)

cnpjs = get_persevera_peers().fund_cnpj.drop_duplicates().tolist()
cvm_data = fds.get_cvm_data(
    cnpjs=cnpjs,
    save_to_db=True
)

from persevera_tools.data.providers.sgs import SGSProvider
sgs_provider = SGSProvider()
sgs_data = sgs_provider.get_data()

sgs_data.to_csv('sgs_data.csv', index=False)
from persevera_tools.data.lookups import get_codes