import os
from datetime import datetime
from persevera_tools.open_finance.pluggy import PluggyService
import logging

print("Running PluggyService example...")

# 1. --- Configuration ---
# IMPORTANT: Replace with your actual credentials.
# It's recommended to use environment variables or a secure config manager.
CLIENT_ID = "0f6c9f58-dccb-4c61-a024-6a36f0e3c9bb"
CLIENT_SECRET = "39e6d7f5-7234-4ecf-bf4d-9f83990b11c0"

# Example Item ID from one institution (e.g., Nubank)
ITEM_ID = "407281a1-010b-4bb1-9782-4aeaa7419b6e"
# -----------------------

# 2. Initialize the service
try:
    print(f"Initializing PluggyService for {ITEM_ID}...")
    pluggy_service = PluggyService(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    print("PluggyService initialized successfully.")

    # 4. Fetch investments data
    print(f"Fetching investments for item: {ITEM_ID}")
    investments = pluggy_service.get_investments(ITEM_ID)
    accounts = pluggy_service.get_accounts(ITEM_ID)
    transactions = pluggy_service.get_transactions(accounts[0]['id'])
    
except ValueError as e:
    print(f"An error occurred during service initialization: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}") 