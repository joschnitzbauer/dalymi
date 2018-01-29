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
        if hasattr(self, 'assert_integrity'):
            self.assert_integrity(data)
        return data

    def _save(self, data, context):
        if hasattr(self, 'assert_integrity'):
            self.assert_integrity(data)
        path = self.loc.format(**context)
        self.save(path, data)

    def assert_integrity(self, data):
        for assertion in self.assertions:
            assertion(data)


class LocalFileMixin:

    def check(self, path):
        return os.path.isfile(path)

    def delete(self, path):
        return os.remove(path)


class PandasDF(Resource):

    def __init__(self, name=None, loc=None, columns=None, custom_assertions=[]):
        assertions = [self.assert_columns] + custom_assertions
        super().__init__(name=name, loc=loc, assertions=assertions)
        self.columns = columns

    def assert_columns(self, df):
        if self.columns is not None:
            assert set(df.columns) == set(self.columns), \
                f'Columns of resource <{self.name}> do not match expected. ' \
                + f'Present: {set(df.columns)}. Expected: {set(self.columns)}.'


class PandasCSV(PandasDF, LocalFileMixin):

    def __init__(self, name=None, loc=None, columns=None, custom_assertions=[]):
        PandasDF.__init__(self, name=name, loc=loc, columns=columns, custom_assertions=custom_assertions)

    def load(self, path):
        return pd.read_csv(path)

    def save(self, path, data):
        return data.to_csv(path, index=False)


class Pickle(Resource, LocalFileMixin):

    def __init__(self, name=None, loc=None, custom_assertions=[]):
        Resource.__init__(self, name=name, loc=loc, assertions=custom_assertions)

    def load(self, path):
        with open(path, 'rb') as f:
            return pickle.load(f)

    def save(self, path, data):
        with open(path, 'wb') as f:
            pickle.dump(data, f)
