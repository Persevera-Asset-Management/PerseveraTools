from persevera_tools.data.providers import ComdinheiroProvider
from datetime import datetime
import pandas as pd

print("Running Comdinheiro provider example...")

# 1. Initialize the provider
provider = ComdinheiroProvider()

# 2. Define parameters
# Use today's date in 'DDMMYYYY' format
analysis_date = datetime.now().strftime('%d%m%Y')

portfolio_names = [
    "ABBR", "ALSA", "ARBB", "BRST"
]

# 3. Fetch data
try:
    print(f"Fetching portfolio positions for date: {analysis_date} and portfolios: {portfolio_names}")
    positions_df = provider.get_data(
        category='portfolio_positions',
        portfolios=portfolio_names,
        date_str=analysis_date
    )

    if not positions_df.empty:
        print("Successfully retrieved portfolio positions:")
        print(positions_df.head())
    else:
        print("No portfolio positions returned.")

except Exception as e:
    print(f"An error occurred: {e}")
