from helper import log_time
from repository import MongoRepository
import pandas as pd


class DataHandler:

    def __init__(self, repository: MongoRepository):
        self.repository = repository

    def generate_dataset(self):
        balance_sheet_df = self.get_balance_sheet_df()
        income_statement_df = self.get_income_statement_df()
        cash_flow_df = self.get_cash_flow_df()

        return [balance_sheet_df, income_statement_df, cash_flow_df]

    @log_time
    def get_balance_sheet_df(self):
        dictionary_list = []
        for document in self._find_all_statement("balance_sheets"):
            for symbol_balance_sheet in document.get("balanceSheets"):
                dictionary_list.append(symbol_balance_sheet)
        return pd.DataFrame.from_dict(dictionary_list)

    def _find_all_statement(self, statement_type: str):
        return self.repository.find_all(statement_type)

    @log_time
    def get_income_statement_df(self):
        dictionary_list = []
        for document in self._find_all_statement("income_statements"):
            for symbol_income_statement in document.get("incomeStatements"):
                dictionary_list.append(symbol_income_statement)
        return pd.DataFrame.from_dict(dictionary_list)

    @log_time
    def get_cash_flow_df(self):
        dictionary_list = []
        for document in self._find_all_statement("cash_flows"):
            for symbol_cash_flow in document.get("cashFlows"):
                dictionary_list.append(symbol_cash_flow)
        return pd.DataFrame.from_dict(dictionary_list)

    @log_time
    def _get_time_series_df(self):
        return self.repository.find_all("time_series_d1")