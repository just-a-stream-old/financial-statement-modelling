import pandas as pd
from sklearn.pipeline import Pipeline
from repository import MongoRepository


class FinancialStatementModellingService:

    def __init__(self, repository: MongoRepository, transform_pipeline: Pipeline):
        self.repository = repository
        self.transform_pipeline = transform_pipeline

    def train_model(self) -> pd.DataFrame:
        balance_sheet_dfs = (pd.DataFrame(list(balance_sheets.get("balanceSheets"))) for balance_sheets in self._get_data())

        # Todo: Extract elsewhere!? ie/ into pipeline etc or data clean transformer
        # balance_sheet_dfs = (df.drop(columns=["link", "finalLink"]) for df in balance_sheet_dfs)

        balance_sheet_dfs_transformed = self.transform_pipeline.fit_transform(balance_sheet_dfs)

        return balance_sheet_dfs_transformed

    def _get_data(self):
        return self.repository.find_all("balance_sheets")
