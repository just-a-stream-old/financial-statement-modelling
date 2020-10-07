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
        # 1. Generate Extracted Features DataFrame From Financial Data
        statement_df_dictionary = self.datahandler.generate_dataset()


        # statements_df_transformed = self.transform_pipeline.transform(statement_dfs) // Post merge

        # 4. Create Test Set

        # 5. Pre-Processing

        # 6. Train Models

        return statement_df_dictionary
