import requests
import json
import pandas as pd
from datetime import datetime
import os
from typing import List, Dict, Any, Optional
from ..utils.logging import get_logger

logger = get_logger(__name__)

class PluggyService:
    """Service to interact with Pluggy API for financial data extraction."""
    
    def __init__(self, client_id: str, client_secret: str):
        """
        Initializes the PluggyService and authenticates to get an API key.

        Args:
            client_id (str): The client ID for Pluggy API.
            client_secret (str): The client secret for Pluggy API.
        """
        self.api_key = self._get_api_key(client_id, client_secret)
        if not self.api_key:
            raise ValueError("Failed to authenticate with Pluggy API. Check credentials.")
        logger.info("Successfully authenticated with Pluggy API.")

    def _convert_dates(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Converts date strings in a list of items to timezone-aware datetimes."""
        date_columns = ["date", "dueDate", "issueDate", "createdAt", "updatedAt"]
        for item in items:
            for col in date_columns:
                if col in item and item[col]:
                    try:
                        item[col] = pd.to_datetime(item[col]).tz_convert(
                            "America/Sao_Paulo"
                        )
                    except (ValueError, TypeError):
                        logger.warning(
                            f"Could not convert value '{item[col]}' in column '{col}' to datetime."
                        )
        return items

    def _get_api_key(self, client_id: str, client_secret: str) -> Optional[str]:
        """
        Authenticates with Pluggy API and returns an API key.

        Args:
            client_id (str): The client ID.
            client_secret (str): The client secret.

        Returns:
            Optional[str]: The API key if authentication is successful, otherwise None.
        """
        auth_url = "https://api.pluggy.ai/auth"
        auth_data = {
            "clientId": client_id,
            "clientSecret": client_secret
        }
        
        response = requests.post(auth_url, json=auth_data)
        if response.status_code == 200:
            return response.json().get("apiKey")
        
        logger.error(f"Failed to get API key. Status: {response.status_code}, Response: {response.text}")
        return None

    def get_accounts(self, item_id: str) -> List[Dict[str, Any]]:
        """
        Retrieves detailed account data for a given item_id.

        Args:
            item_id (str): The ID of the item to retrieve accounts for.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing an account.
        """
        logger.info(f"Extracting account data for item_id: {item_id}")
        url = "https://api.pluggy.ai/accounts"
        headers = {"X-API-KEY": self.api_key, "Content-Type": "application/json"}
        params = {"itemId": item_id}
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            accounts = response.json().get("results", [])
            return self._convert_dates(accounts)
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching accounts for item_id {item_id}: {e}")
            return []

    def get_investments(self, item_id: str) -> List[Dict[str, Any]]:
        """
        Retrieves detailed investment data for a given item_id.

        Args:
            item_id (str): The ID of the item to retrieve investments for.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing an investment.
        """
        logger.info(f"Extracting investment data for item_id: {item_id}")
        url = "https://api.pluggy.ai/investments"
        headers = {"X-API-KEY": self.api_key, "Content-Type": "application/json"}
        params = {"itemId": item_id}

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            investments = response.json().get("results", [])
            return self._convert_dates(investments)
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching investments for item_id {item_id}: {e}")
            return []

    def get_transactions(self, account_id: str) -> List[Dict[str, Any]]:
        """
        Retrieves detailed transaction data for a given account_id.

        Args:
            account_id (str): The ID of the account to retrieve transactions for.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing a transaction.
        """
        logger.info(f"Extracting transaction data for account_id: {account_id}")
        url = "https://api.pluggy.ai/transactions"
        headers = {"X-API-KEY": self.api_key, "Content-Type": "application/json"}
        params = {"accountId": account_id}
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            transactions = response.json().get("results", [])
            return self._convert_dates(transactions)
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching transactions for account_id {account_id}: {e}")
            return []
