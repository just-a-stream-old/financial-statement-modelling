import pandas as pd
from sklearn.pipeline import Pipeline

from helper import log_time
from repository import MongoRepository


class FinancialStatementModellingService:

    def __init__(self, repository: MongoRepository, transform_pipeline: Pipeline):
        self.repository = repository
        self.transform_pipeline = transform_pipeline

    def run_service(self) -> list:
        statement_dfs = self._generate_dataset()
        # statements_df_transformed = [self.transform_pipeline.transform(df) for df in statement_dfs]
        return statement_dfs

    def _generate_dataset(self):
        # 1. Get Statement DataFrames
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

    def _generate_features(self):
        return self
