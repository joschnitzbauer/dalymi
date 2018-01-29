from abc import ABC, abstractmethod
import os.path
import pickle


class Resource(ABC):

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

    @abstractmethod
    def check(self, path):
        pass

    def delete(self, path):
        msg = f'Could not delete resource <{self.name}> (and possibly others), '
        msg += 'because the class does not override the `delete` method.'
        raise NotImplementedError(msg)

    @abstractmethod
    def load(self, path):
        pass

    @abstractmethod
    def save(self, path, data):
        pass


class LocalFileMixin:
    ''' Provides default `check` and `delete` methods for local file resources. Inherit from this before any abstract class.'''

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


class PandasCSV(LocalFileMixin, PandasDF):

    def __init__(self, name=None, loc=None, columns=None, custom_assertions=[]):
        PandasDF.__init__(self, name=name, loc=loc, columns=columns, custom_assertions=custom_assertions)

    def load(self, path):
        import pandas as pd  # importing pandas here to avoid general dependency on it
        return pd.read_csv(path)

    def save(self, path, data):
        dirs = os.path.dirname(path)
        if dirs:
            os.makedirs(dirs, exist_ok=True)
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
