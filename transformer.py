from typing import Generator
from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd


class DropColumnsTransformer(BaseEstimator, TransformerMixin):

    def __init__(self, columns_to_drop: list):
        self.columns_to_drop = columns_to_drop

    def fit(self, x: Generator[pd.DataFrame, None, None], y: Generator = None):
        return self

    def transform(self, x: Generator[pd.DataFrame, None, None], y: Generator = None):
        return (df.drop(columns=self.columns_to_drop) for df in x)



