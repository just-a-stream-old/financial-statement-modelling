from functools import reduce
import pandas as pd
from sklearn.pipeline import Pipeline

from datahandler import DataHandler
from helper import log_time


class FinancialStatementModellingService:

    def __init__(self, datahandler: DataHandler, transform_pipeline: Pipeline):
        self.datahandler = datahandler
        self.transform_pipeline = transform_pipeline

    def run_service(self) -> list:
        # 1. Get Pre-Feature Data Dictionary
        statement_df_dictionary = self.datahandler.generate_dataset()

        # 2. Feature Engineering Per Financial Statement

        # 3. Merge Financial Statement Related Feature DataFrames
        # statements_df = self.merge_dataframes(
        #     [self.get_balance_sheet_df(), self.get_income_statement_df(), self.get_cash_flow_df()],
        #     merge_on_columns=["symbol", "date", "period"]
        # )
        # statements_df_transformed = self.transform_pipeline.transform(statement_dfs) // Post merge

        # 4. Create Test Set

        # 5. Pre-Processing

        # 6. Train Models

        return statement_df_dictionary

    @staticmethod
    @log_time
    def merge_dataframes(dataframes, merge_on_columns):
        return reduce(
            lambda left, right: pd.merge(
                left, right,
                how="inner", on=merge_on_columns
            ),
            dataframes
        )

    def _generate_features(self):
        return self
