from bcb import Expectativas
import pandas as pd

from .base import DataProvider, DataRetrievalError, ValidationError


class BcbFocusProvider(DataProvider):
    """Provider for BCB Focus Expectations data."""

    def __init__(self, start_date: str = "2015-01-01"):
        """
        Initialize the BCB Focus provider.

        Args:
            start_date: The start date for data retrieval
        """
        super().__init__(start_date)
        try:
            self.em = Expectativas()
        except Exception as e:
            raise DataRetrievalError(f"Failed to initialize BCB Expectativas: {e}")

    def _get_selic_expectations(self) -> pd.DataFrame:
        """Retrieve Selic market expectations."""
        self.logger.info("Retrieving Selic expectations from BCB Focus...")
        try:
            ep = self.em.get_endpoint("ExpectativasMercadoSelic")
            df = (
                ep.query()
                .filter(ep.baseCalculo == 0)
                .filter(ep.Data >= self.start_date.strftime("%Y-%m-%d"))
                .collect()
            )
        except Exception as e:
            raise DataRetrievalError(
                f"Failed to retrieve Selic expectations from BCB Focus: {e}"
            )

        if df.empty:
            self.logger.warning("No Selic expectations data retrieved.")
            return pd.DataFrame()

        df = df.drop(columns="baseCalculo")
        df.columns = [
            "indicator",
            "date",
            "reference_date",
            "average",
            "median",
            "std",
            "minimum",
            "maximum",
            "total_responses",
        ]

        df["reference"] = df["reference_date"].str[-4:] + df["reference_date"].str[:2]
        df["code"] = "br_focus_selic_median_" + df["reference"]

        df = df[["code", "date", "median"]]

        long_df = df.melt(id_vars=["code", "date"], var_name="field", value_name="value")
        long_df["field"] = "close"

        return long_df

    def _get_ipca_expectations(self) -> pd.DataFrame:
        """Retrieve annual IPCA market expectations."""
        self.logger.info("Retrieving annual IPCA expectations from BCB Focus...")
        try:
            ep = self.em.get_endpoint("ExpectativasMercadoAnuais")
            df = (
                ep.query()
                .filter(ep.Indicador == "IPCA")
                .filter(ep.baseCalculo == 0)
                .filter(ep.Data >= self.start_date.strftime("%Y-%m-%d"))
                .collect()
            )
        except Exception as e:
            raise DataRetrievalError(
                f"Failed to retrieve IPCA expectations from BCB Focus: {e}"
            )

        if df.empty:
            self.logger.warning("No IPCA expectations data retrieved.")
            return pd.DataFrame()

        df = df.drop(columns=["baseCalculo", "IndicadorDetalhe"])
        df.columns = [
            "indicator",
            "date",
            "reference",
            "average",
            "median",
            "std",
            "minimum",
            "maximum",
            "total_responses",
        ]

        df["code"] = "br_focus_ipca_median_" + df["reference"]

        df = df[["code", "date", "median"]]

        long_df = df.melt(id_vars=["code", "date"], var_name="field", value_name="value")
        long_df["field"] = "close"

        return long_df

    def get_data(self, **kwargs) -> pd.DataFrame:
        """
        Retrieve data from BCB Focus for Selic and IPCA.

        Returns:
            DataFrame with columns: ['date', 'code', 'field', 'value']
        """
        self._log_processing("bcb_focus")

        selic_df = self._get_selic_expectations()
        ipca_df = self._get_ipca_expectations()

        if selic_df.empty and ipca_df.empty:
            raise DataRetrievalError("No data retrieved from BCB Focus")

        df = pd.concat([selic_df, ipca_df], ignore_index=True)

        return self._validate_output(df)
