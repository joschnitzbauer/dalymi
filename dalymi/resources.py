import os.path
import pickle

import pandas as pd


class Resource:

    def __init__(self, name=None, loc=None, assertions=[]):
        self.name = name
        self.loc = loc
        self.assertions = assertions

    def _check(self, context):
        path = self.loc.format(**context)
        return self.check(path)

    def _delete(self, context):
        path = self.loc.format(**context)
        self.delete(path)

    def _load(self, context):
        path = self.loc.format(**context)
        data = self.load(path)
        self.assert_integrity(data)
        return data

    def _save(self, data, context):
        self.assert_integrity(data)
        path = self.loc.format(**context)
        self.save(path, data)

    def assert_integrity(self, data):
        for assertion in self.assertions:
            assertion(data)


class LocalFile(Resource):

    def check(self, path):
        return os.path.isfile(path)

    def delete(self, path):
        return os.remove(path)


class PandasCSV(LocalFile):

    def __init__(self, name=None, loc=None, columns=[], custom_assertions=[]):
        assertions = [self.assert_columns] + custom_assertions
        super().__init__(name, loc, assertions=assertions)
        self.columns = columns

    def assert_columns(self, df):
        if len(self.columns):
            assert set(df.columns) == set(self.columns), \
                f'Columns of resource <{self.name}> do not match expected. ' \
                + f'Present: {set(df.columns)}. Expected: {set(self.columns)}.'

    def load(self, path):
        return pd.read_csv(path)

    def save(self, path, data):
        return data.to_csv(path, index=False)


class PickleResource(LocalFile):

    def __init__(self, name=None, loc=None, custom_assertions=[]):
        super().__init__(name, loc, assertions=custom_assertions)

    def load(self, path):
        with open(path, 'rb') as f:
            return pickle.load(f)

    def save(self, path, data):
        with open(path, 'wb') as f:
            pickle.dump(data, f)
