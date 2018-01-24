import os.path
import pickle

import pandas as pd


class PipelineResource:

    def __init__(self, name=None, loc=None):
        self.name = name
        self.loc = loc

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


class AssertedResource:

    def __init__(self, assertions=[]):
        self.assertions = assertions

    def assert_integrity(self, data):
        for assertion in self.assertions:
            assertion(data)


class LocalFileMixin:

    def check(self, path):
        return os.path.isfile(path)

    def delete(self, path):
        return os.remove(path)


class PandasDF(AssertedResource):

    def __init__(self, columns=[], custom_assertions=[]):
        assertions = [self.assert_columns] + custom_assertions
        super().__init__(assertions=assertions)
        self.columns = columns

    def assert_columns(self, df):
        if len(self.columns):
            assert set(df.columns) == set(self.columns), \
                f'Columns of resource <{self.name}> do not match expected. ' \
                + f'Present: {set(df.columns)}. Expected: {set(self.columns)}.'


class PandasCSV(PandasDF, PipelineResource, LocalFileMixin):

    def __init__(self, name, loc, columns=[], custom_assertions=[]):
        PandasDF.__init__(self, columns, custom_assertions)
        PipelineResource.__init__(self, name=name, loc=loc)

    def load(self, path):
        return pd.read_csv(path)

    def save(self, path, data):
        return data.to_csv(path, index=False)


class Pickle(AssertedResource, PipelineResource, LocalFileMixin):

    def __init__(self, name, loc, custom_assertions=[]):
        AssertedResource.__init__(self, assertions=custom_assertions)
        PipelineResource.__init__(self, name=name, loc=loc)

    def load(self, path):
        with open(path, 'rb') as f:
            return pickle.load(f)

    def save(self, path, data):
        with open(path, 'wb') as f:
            pickle.dump(data, f)
