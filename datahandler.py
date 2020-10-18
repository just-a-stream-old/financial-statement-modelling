from functools import reduce

from feature_engineering import FeatureExtractor
from helper import log_time, map_to_weekday_datetime, parse_datetime_object
from repository import MongoRepository
import pandas as pd


class DataHandler:

    def __init__(self, repository: MongoRepository, feature_extractor: FeatureExtractor):
        self.repository = repository
        self.feature_extractor = feature_extractor

    def generate_dataset(self):
        # Query Repository For Financials
        financials_df_dictionary = {
            "balance_sheet_df": self._get_balance_sheet_df(),
            "income_statement_df": self._get_income_statement_df(),
            "cash_flow_df": self._get_cash_flow_df()
        }
        # Extract Features From Financials
        df_dict = self.feature_extractor.extract_features(financials_df_dictionary)

        # Query Repository For Time Series, Extract Labels, & Add To DataFrame Dictionary
        df_dict["time_series_labels_df"] = self.feature_extractor.extract_labels(
            self._get_time_series_df(),
            comparison_symbol="SPY"
        )

        return self.merge_dataframes(df_dict.values(), merge_on_columns=["symbol", "date"])

    @log_time
    def _get_balance_sheet_df(self) -> pd.DataFrame:
        dictionary_list = []
        for document in self._find_all_statement("balance_sheets"):
            for symbol_balance_sheet in document.get("balanceSheets"):
                symbol_balance_sheet.update({"date": map_to_weekday_datetime(symbol_balance_sheet.get("date"))})
                dictionary_list.append(symbol_balance_sheet)
        return pd.DataFrame.from_dict(dictionary_list)

    def _find_all_statement(self, statement_type: str):
        return self.repository.find_all(statement_type)

    @log_time
    def _get_income_statement_df(self) -> pd.DataFrame:
        dictionary_list = []
        for document in self._find_all_statement("income_statements"):
            for symbol_income_statement in document.get("incomeStatements"):
                symbol_income_statement.update({"date": map_to_weekday_datetime(symbol_income_statement.get("date"))})
                dictionary_list.append(symbol_income_statement)
        return pd.DataFrame.from_dict(dictionary_list)

    @log_time
    def _get_cash_flow_df(self) -> pd.DataFrame:
        dictionary_list = []
        for document in self._find_all_statement("cash_flows"):
            for symbol_cash_flow in document.get("cashFlows"):
                symbol_cash_flow.update({"date": map_to_weekday_datetime(symbol_cash_flow.get("date"))})
                dictionary_list.append(symbol_cash_flow)
        return pd.DataFrame.from_dict(dictionary_list)

    @log_time
    def _get_time_series_df(self) -> pd.DataFrame:
        dictionary_list = []
        for document in self._find_all_statement("time_series_d1"):

            if self.is_valid_timeseries_document(document) is False:
                continue

            closes_ma = self.determine_adjusted_close_ma(document, window_size=14)

            # Todo: Determine labels here so we don't have to groupby and re-merge!

            for candle, close in zip(document.get("timeSeries"), closes_ma):

                dictionary_list.append(
                    {
                        "symbol": document.get("symbol"),
                        "date": parse_datetime_object(candle.get("timestamp")),
                        "adjusted_close_ma": close
                    }
                )
            del closes_ma, document

        return pd.DataFrame.from_dict(dictionary_list)

    @staticmethod
    def is_valid_timeseries_document(document: dict) -> bool:
        is_valid = True

        if document is None:
            is_valid = False

        if len(document.get("timeSeries")) == 0:
            is_valid = False

        if is_valid is False:
            # Todo: Delete from MongoDB please
            print(f"Method: is_valid_timeseries_document() returns invalid for document: {str(document)}")

        return is_valid

    @staticmethod
    def determine_adjusted_close_ma(document: dict, window_size: int) -> pd.Series:
        closes = pd.Series([candle.get("adjustedClose") for candle in document.get("timeSeries")])
        return closes.rolling(window_size).mean().fillna(method="backfill")

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
