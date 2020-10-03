import pandas as pd
from sklearn.pipeline import Pipeline
from repository import MongoRepository


class FinancialStatementModellingService:

    def __init__(self, repository: MongoRepository, transform_pipeline: Pipeline):
        self.repository = repository
        self.transform_pipeline = transform_pipeline

    def run_service(self) -> pd.DataFrame:
        statements_df = self._generate_dataset()
        statements_df_transformed = self.transform_pipeline.transform(statements_df)
        return statements_df_transformed

    def _generate_dataset(self):
        # 1. Get Statement DataFrames
        balance_sheet_df = self._get_balance_sheet_df()
        income_statement_df = self._get_income_statement_df()
        cash_flow_df_generator = self._get_cash_flow_df()

        return balance_sheet_df

    def _get_balance_sheet_df(self):
        balance_sheet_df = pd.DataFrame()
        for balance_sheets in self._find_all_statement("balance_sheets"):
            balance_sheet_df = pd.concat([
                balance_sheet_df,
                pd.DataFrame(list(balance_sheets.get("balanceSheets")))
            ])
        return balance_sheet_df

    def _find_all_statement(self, statement_type: str):
        return self.repository.find_all(statement_type);

    def _get_income_statement_df(self):
        income_statement_df = pd.DataFrame()
        for income_statement in self._find_all_statement("income_statement"):
            income_statement_df = pd.concat([
                income_statement_df,
                pd.DataFrame(list(income_statement.get("incomeStatements")))
            ])
        return income_statement_df

    def _get_cash_flow_df(self):
        cash_flow_df = pd.DataFrame()
        for cash_flow in self._find_all_statement("cash_flow"):
            cash_flow_df = pd.concat([
                cash_flow_df,
                pd.DataFrame(list(cash_flow.get("cashFlows")))
            ])
        return cash_flow_df

    def _get_time_series_df(self):
        return self.repository.find_all("time_series_d1")

    def _generate_features(self):
        return self
