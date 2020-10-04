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



