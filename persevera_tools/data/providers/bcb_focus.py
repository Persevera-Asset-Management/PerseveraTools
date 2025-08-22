from bcb import Expectativas
import pandas as pd
import unicodedata

from .base import DataProvider, DataRetrievalError, ValidationError


class BcbFocusProvider(DataProvider):
    """Provider for BCB Focus Expectations data."""

    @staticmethod
    def _slugify(text: str) -> str:
        """
        Normalize string by removing accents, converting to lowercase,
        replacing spaces with underscores and removing hyphens.
        """
        text = unicodedata.normalize("NFD", text)
        text = "".join(c for c in text if unicodedata.category(c) != "Mn")
        return text.lower().replace(" ", "_").replace("-", "")

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

    def _get_annual_expectations(self, indicator: str) -> pd.DataFrame:
        """Retrieve annual market expectations."""
        self.logger.info(f"Retrieving annual {indicator} expectations from BCB Focus...")
        try:
            ep = self.em.get_endpoint("ExpectativasMercadoAnuais")
            df = (
                ep.query()
                .filter(ep.Indicador == indicator)
                .filter(ep.baseCalculo == 0)
                .filter(ep.Data >= self.start_date.strftime("%Y-%m-%d"))
                .collect()
            )
        except Exception as e:
            raise DataRetrievalError(
                f"Failed to retrieve {indicator} expectations from BCB Focus: {e}"
            )

        if df.empty:
            self.logger.warning(f"No {indicator} expectations data retrieved.")
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

        df["code"] = f"br_focus_{self._slugify(indicator)}_median_" + df["reference"]

        df = df[["code", "date", "median"]]

        long_df = df.melt(id_vars=["code", "date"], var_name="field", value_name="value")
        long_df["field"] = "close"

        return long_df
    
    def _get_monthly_expectations(self, indicator: str) -> pd.DataFrame:
        """Retrieve monthly market expectations."""
        self.logger.info(f"Retrieving monthly {indicator} expectations from BCB Focus...")
        try:
            ep = self.em.get_endpoint("ExpectativaMercadoMensais")
            df = (
                ep.query()
                .filter(ep.Indicador == indicator)
                .filter(ep.baseCalculo == 0)
                .filter(ep.Data >= self.start_date.strftime("%Y-%m-%d"))
                .collect()
            )
        except Exception as e:
            raise DataRetrievalError(
                f"Failed to retrieve {indicator} expectations from BCB Focus: {e}"
            )

        if df.empty:
            self.logger.warning(f"No {indicator} expectations data retrieved.")
            return pd.DataFrame()

        df = df.drop(columns=["baseCalculo"])
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

        df["code"] = f"br_focus_{self._slugify(indicator)}_median_" + (df["reference"].str[3:] + df["reference"].str[:2])

        df = df[["code", "date", "median"]]

        long_df = df.melt(id_vars=["code", "date"], var_name="field", value_name="value")
        long_df["field"] = "close"

        return long_df

    def get_data(self, category: str, **kwargs) -> pd.DataFrame:
        """
        Retrieve data from BCB Focus.

        Returns:
            DataFrame with columns: ['date', 'code', 'field', 'value']
        """
        self._log_processing(category)

        selic_df = self._get_selic_expectations()
        ipca_monthly_df = self._get_monthly_expectations("IPCA")
        ipca_annual_df = self._get_annual_expectations("IPCA")
        us_dollar_df = self._get_annual_expectations("Câmbio")
        pib_df = self._get_annual_expectations("PIB Total")

        if selic_df.empty and ipca_monthly_df.empty and ipca_annual_df.empty and us_dollar_df.empty and pib_df.empty:
            raise DataRetrievalError("No data retrieved from BCB Focus")

        df = pd.concat([selic_df, ipca_monthly_df, ipca_annual_df, us_dollar_df, pib_df], ignore_index=True)

        return self._validate_output(df)
