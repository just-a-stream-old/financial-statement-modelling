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
        balance_sheet_df_generator = self._get_balance_sheet_df()
        income_statement_df_generator = self._get_income_statement_df()
        cash_flow_df_generator = self._get_cash_flow_df()

        # 2. Combine all
        # return pd.merge(balance_sheet_df, income_statement_df).merge(cash_flow_df)

        # 3. Feature engineering

        # 4. Add labels

    def _get_balance_sheet_df(self):
        return (pd.DataFrame(list(balance_sheets.get("balanceSheets"))) for balance_sheets in
                self._find_all_statement("balance_sheets"))

    def _find_all_statement(self, statement_type: str):
        return self.repository.find_all(statement_type);

    def _get_income_statement_df(self):
        return (pd.DataFrame(list(income_statements.get("incomeStatements"))) for income_statements in
                self._find_all_statement("income_statements"))

    def _get_cash_flow_df(self):
        return (pd.DataFrame(list(cash_flows.get("cashFlows"))) for cash_flows in
                self._find_all_statement("cash_flows"))

    def _generate_features(self):
        return self
