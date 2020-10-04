from sklearn.pipeline import Pipeline

from datahandler import DataHandler


class FinancialStatementModellingService:

    def __init__(self, datahandler: DataHandler, transform_pipeline: Pipeline):
        self.datahandler = datahandler
        self.transform_pipeline = transform_pipeline

    def run_service(self) -> list:
        statement_dfs = self.datahandler.generate_dataset()
        statements_df_transformed = [self.transform_pipeline.transform(df) for df in statement_dfs]
        return statements_df_transformed

    def _generate_features(self):
        return self
