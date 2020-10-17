from functools import reduce

from feature_engineering import FeatureExtractor
from helper import log_time, map_to_weekday_datetime
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
            # "cash_flow_df": self._get_cash_flow_df()
        }
        # Extract Features From Financials
        # df_dict = self.feature_extractor.extract_features(financials_df_dictionary)

        # Query Repository For Time Series
        # time_series_df = self._get_time_series_df(
        # financials_df_dictionary.get("cash_flow_df")["symbol"].unique().tolist(),
        # financials_df_dictionary.get("cash_flow_df")["date"].tolist()
        # list({"AAPL", "AAPL", "AAPL", "AAPL", "AAPL", "AAPL", "AAPL"}),
        # ["2020-03-28", "2018-12-29", "2018-06-30", "2011-06-25", "2005-06-25", "1993-06-25", "1991-03-29"]
        # )

        # Extract Labels From Time Series

        # return self.merge_dataframes(df_dict.values(), merge_on_columns=["symbol", "date", "period"])
        # return time_series_df
        return self._get_cash_flow_df()

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
                symbol_cash_flow.update({"date": map_to_weekday_datetime(symbol_cash_flow.get("date"))})
                dictionary_list.append(symbol_cash_flow)
        return pd.DataFrame.from_dict(dictionary_list)

    @log_time
    def _get_time_series_df(self, symbols: list, dates: list) -> pd.DataFrame:
        dictionary_list = []
        for document in (self.repository.find_one("time_series_d1", {"symbol": symbol}) for symbol in symbols):

            if self.is_valid_timeseries_document(document) is False:
                continue

            closes_7d_ma = self.determine_adjusted_close_ma(document, window_size=7)

            for index, (candle, close) in enumerate(zip(document.get("timeSeries"), closes_7d_ma)):
                dictionary_list.extend(
                    self.find_matching_close_for_date(document, closes_7d_ma, dates, index, candle, close))
                continue

            del closes_7d_ma, document

        return pd.DataFrame.from_dict(dictionary_list)


    @staticmethod
    def is_valid_timeseries_document(document: dict) -> bool:
        is_valid = True

        if document is None:
            is_valid = False

        if len(document.get("timeSeries")) == 0:
            is_valid = False

        if is_valid is False:
            print(f"Method: is_valid_timeseries_document() returns invalid for document: {str(document)}")

        return is_valid

    @staticmethod
    def determine_adjusted_close_ma(document: dict, window_size: int) -> pd.Series:
        closes = pd.Series([time_series.get("adjustedClose") for time_series in document.get("timeSeries")])
        return closes.rolling(window_size).mean().fillna(method="backfill")

    @staticmethod
    def find_matching_close_for_date(document: dict, closes_7d_ma: list, dates: list, index: int, candle: dict,
                                     close: float):

        dictionary_list = []
        for date in dates:

            day_of_the_week = datetime.strptime(date, "%Y-%m-%d").weekday()

            if day_of_the_week < 5:

                if date == candle.get("timestamp"):
                    dictionary_list.append(
                        {
                            "symbol": document.get("symbol"),
                            "date": candle.get("timestamp"),
                            "adjusted_close_ma": close
                        }
                    )
                    print(f"Success at index: {index}")


                else:
                    # Todo: Add logging here at debug level
                    continue

            else:
                try:
                    dictionary_list.append(
                        {
                            "symbol": document.get("symbol"),
                            "date": candle.get("timestamp"),
                            "adjusted_close_ma": closes_7d_ma[index + (day_of_the_week - 7)]
                        }
                    )
                    # print(f"Success at index: {index} using dummy monday index: {index + (day_of_the_week - 7)}")
                except:
                    dictionary_list.append(
                        {
                            "symbol": document.get("symbol"),
                            "date": candle.get("timestamp"),
                            "adjusted_close_ma": closes_7d_ma[index + (7 - day_of_the_week)]
                        }
                    )
                    # print(f"Success at index: {index} using dummy monday index: {index + (7 - day_of_the_week)}")

        return dictionary_list

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
