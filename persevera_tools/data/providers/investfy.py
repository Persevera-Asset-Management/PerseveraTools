from typing import Dict

import pandas as pd
import requests

from .base import DataProvider, DataRetrievalError


class InvestfyProvider(DataProvider):
    """Provider for daily B3 investor flow data from Investfy."""

    API_URL = "https://fluxos.investfy.com/api/investors"
    CATEGORY = "investfy_investor_flow"

    INVESTOR_CODE_MAP: Dict[str, str] = {
        "financial_institutions": "financial_institutions",
        "foreigners": "foreign_investors",
        "individuals": "individual_investors",
        "institutional": "institutional_investors",
        "other": "others",
        "clubs": "clubs",
        "companies": "companies",
    }

    def __init__(self, start_date: str = "1980-01-01"):
        super().__init__(start_date)

    def get_data(self, category: str, **kwargs) -> pd.DataFrame:
        """Retrieve data from Investfy and return standardized output."""
        self._log_processing(category)

        if category != self.CATEGORY:
            raise ValueError(f"Invalid category: {category}")

        try:
            response = requests.get(self.API_URL, timeout=30)
            response.raise_for_status()
            payload = response.json() or {}
        except Exception as exc:
            raise DataRetrievalError(f"Failed to retrieve data from Investfy: {exc}") from exc

        rows = payload.get("data") or []
        if not rows:
            raise DataRetrievalError("No data retrieved from Investfy")

        raw_df = pd.DataFrame(rows)
        if raw_df.empty or "date" not in raw_df.columns:
            raise DataRetrievalError("Investfy payload does not contain expected fields")

        flow_fields = [field for field in self.INVESTOR_CODE_MAP if field in raw_df.columns]
        if not flow_fields:
            raise DataRetrievalError("Investfy payload does not contain investor flow columns")

        df = raw_df[["date"] + flow_fields].melt(
            id_vars=["date"],
            value_vars=flow_fields,
            var_name="investor_type",
            value_name="value",
        )
        df = df.dropna(subset=["value"])
        df["code"] = (
            "br_b3_"
            + df["investor_type"].replace(self.INVESTOR_CODE_MAP)
            + "_net"
        )
        df["field"] = "close"

        final_df = df[["date", "code", "field", "value"]]
        return self._validate_output(final_df)
