from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd


# Todo: Go through and sort out type hints, the types are not correct on the transformers...

class TestTransformer(BaseEstimator, TransformerMixin):

    def __init__(self, do_dummy_transform: bool = True):
        self.do_dummy_transform = do_dummy_transform

    def fit(self, x: pd.DataFrame, y: pd.DataFrame = None):
        return self

    def transform(self, x: pd.DataFrame, y: pd.DataFrame = None):
        if self.do_dummy_transform:
            return x


class DropColumnsTransformer(BaseEstimator, TransformerMixin):

    def __init__(self, columns_to_drop: list):
        self.columns_to_drop = columns_to_drop

    def fit(self, x: pd.DataFrame, y: pd.DataFrame = None):
        return self

    def transform(self, x: pd.DataFrame, y: pd.DataFrame = None):
        return (df.drop(columns=self.columns_to_drop) for df in x)
