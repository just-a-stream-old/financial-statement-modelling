from typing import Generator
from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd


class DropColumnsTransformer(BaseEstimator, TransformerMixin):

    def __init__(self, columns_to_drop: list):
        self.columns_to_drop = columns_to_drop

    def fit(self, x: pd.DataFrame, y: pd.DataFrame = None):
        return self

    def transform(self, x: pd.DataFrame, y: pd.DataFrame = None):
        return x.drop(columns=self.columns_to_drop, axis=1)


class DropDuplicateColumns(BaseEstimator, TransformerMixin):

    def __init__(self, duplicate_column_suffixes=None):
        if duplicate_column_suffixes is None:
            duplicate_column_suffixes = ["_y", "_z"]
        self.duplicate_column_suffixes = duplicate_column_suffixes

    def fit(self, x: pd.DataFrame, y: pd.DataFrame = None):
        return self

    def transform(self, x: pd.DataFrame, y: pd.DataFrame = None):
        columns_to_drop = []
        for suffix in self.duplicate_column_suffixes:
            for column in x.columns:
                if suffix in column:
                    columns_to_drop.append(column)

        return x.drop(columns=columns_to_drop)


class DropColumnNameSuffix(BaseEstimator, TransformerMixin):

    def __init__(self, suffixes_to_remove: list):
        self.suffixes_to_remove = suffixes_to_remove

    def fit(self, x: pd.DataFrame, y: pd.DataFrame = None):
        return self

    def transform(self, x: pd.DataFrame, y: pd.DataFrame = None):
        transformed_columns = []
        for suffix in self.suffixes_to_remove:
            for column in x.columns:
                if suffix in column:
                    transformed_columns.append(column.replace(suffix, ""))
                else:
                    transformed_columns.append(column)

        x.columns = transformed_columns
        return x
