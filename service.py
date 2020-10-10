from functools import reduce
import pandas as pd
from sklearn.model_selection import StratifiedShuffleSplit
from sklearn.pipeline import Pipeline

from datahandler import DataHandler
from helper import log_time


class FinancialStatementModellingService:

    def __init__(self, datahandler: DataHandler, transform_pipeline: Pipeline, dataset_splitter: StratifiedShuffleSplit):
        self.datahandler = datahandler
        self.dataset_splitter = dataset_splitter
        self.transform_pipeline = transform_pipeline

    def train_model(self) -> list:
        # 1 Generate Extracted Features DataFrame From Financial Data
        features_df = self.datahandler.generate_dataset()

        # 2 Create Training & Test Sets
        # train_set, test_set = self.generate_test_set(features_df)

        # 3 Pre-Processing Transformations


        # 4 Train Models


        return features_df

    def generate_test_set(self, data: pd.DataFrame):
        for train_index, test_index in self.dataset_splitter.split(data, data["symbol"]):
            train_set = data.loc[train_index]
            test_set = data.loc[test_index]
        print(f"generate_test_set(): Training set created with length: {len(train_set)}")
        print(f"generate_test_set(): Test set created with length: {len(test_set)}")

        return train_set, test_set






