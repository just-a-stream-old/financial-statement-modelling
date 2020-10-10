from functools import reduce

from feature_engineering import FeatureExtractor
from helper import log_time
from repository import MongoRepository
import pandas as pd


class DataHandler:

    def __init__(self, repository: MongoRepository, feature_extractor: FeatureExtractor):
        self.repository = repository
        self.feature_extractor = feature_extractor

    def generate_dataset(self):
        # Query Repository For Financials
        financials_df_dictionary = {
            # "balance_sheet_df": self._get_balance_sheet_df(),
            # "income_statement_df": self._get_income_statement_df(),
            "cash_flow_df": self._get_cash_flow_df()
        }
        # Extract Features From Financials
        # df_dict = self.feature_extractor.extract_features(financials_df_dictionary)

        # Query Repository For Time Series
        time_series_df = self._get_time_series_df(
            financials_df_dictionary.get("cash_flow_df")["symbol"].tolist(),
            financials_df_dictionary.get("cash_flow_df")["date"].tolist()
        )

        # Extract Labels From Time Series

        # return self.merge_dataframes(df_dict.values(), merge_on_columns=["symbol", "date", "period"])
        return time_series_df

    @log_time
    def _get_balance_sheet_df(self) -> pd.DataFrame:
        dictionary_list = []
        for document in self._find_all_statement("balance_sheets"):
            for symbol_balance_sheet in document.get("balanceSheets"):
                dictionary_list.append(symbol_balance_sheet)
        return pd.DataFrame.from_dict(dictionary_list)

    def _find_all_statement(self, statement_type: str):
        return self.repository.find_all(statement_type)

    @log_time
    def _get_income_statement_df(self) -> pd.DataFrame:
        dictionary_list = []
        for document in self._find_all_statement("income_statements"):
            for symbol_income_statement in document.get("incomeStatements"):
                dictionary_list.append(symbol_income_statement)
        return pd.DataFrame.from_dict(dictionary_list)

    @log_time
    def _get_cash_flow_df(self) -> pd.DataFrame:
        dictionary_list = []
        for document in self._find_all_statement("cash_flows"):
            for symbol_cash_flow in document.get("cashFlows"):
                dictionary_list.append(symbol_cash_flow)
        return pd.DataFrame.from_dict(dictionary_list)

    @log_time
    def _get_time_series_df(self, symbols: list, dates: list) -> pd.DataFrame:
        dictionary_list = []

        queries = (self.repository.find_one("time_series_d1", {"symbol": symbol}) for symbol in symbols)

        for document, date in zip(queries, dates):

            for candle in document.get("timeSeries"):

                if candle.get("timestamp") == date:
                    dictionary_list.append(
                        {
                            "symbol": document.get("symbol"),
                            "date": candle.get("timestamp"),
                            "adjusted_close": candle.get("adjustedClose")
                        }
                    )

        return pd.DataFrame.from_dict(dictionary_list)

    @staticmethod
    @log_time
    def merge_dataframes(dataframes, merge_on_columns: list, merge_how: str = "inner") -> pd.DataFrame:
        return reduce(
            lambda left, right: pd.merge(
                left, right,
                how=merge_how, on=merge_on_columns
            ),
            dataframes
        )
