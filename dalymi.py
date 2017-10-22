import argparse
from functools import wraps
import os.path
import pprint

import pandas as pd
import numpy as np


class Resource:

    def __init__(self, name, loc, load=pd.read_csv, save=lambda df, path: df.to_csv(path), check=os.path.isfile,
                 columns=None, custom_assertions=[]):
        self.name = name
        self.loc = loc
        self._load = load
        self._save = save
        self._check = check
        self.columns = columns
        self.custom_assertions = custom_assertions

    def __repr__(self):
        return self.name

    def assert_integrity(self, df):
        self.assert_columns(df)
        for custom_assertion in self.custom_assertions:
            custom_assertion(df)

    def assert_columns(self, df):
        if self.columns is not None:
            assert set(df.columns) == set(self.columns), \
                f'Columns of resource <{self.name}> do not match expected. ' \
                + f'Present: {set(df.columns)}. Expected: {set(self.columns)}.'

    def check(self, context):
        path = self.loc.format(**context)
        return self._check(path)

    def load(self, context):
        path = self.loc.format(**context)
        df = self._load(path)
        self.assert_integrity(df)
        return df

    def save(self, df, context):
        self.assert_integrity(df)
        path = self.loc.format(**context)
        self._save(df, path)


class Pipeline:

    def __init__(self, verbose_during_setup=False):
        self.outputs = {}    # keys: funcs, values: list of output resources
        self.producers = {}  # keys: resources, values: funcs
        self.funcs = {}      # keys: func names, values: funcs
        self.verbose_during_setup = verbose_during_setup

    def _create_input_wrapper(self, func, input):

        @wraps(func)
        def func_wrapped(**context):
            missing = [_ for _ in input if not _.check(context)]
            producers_missing = [self.producers[_] for _ in missing]
            for producer in producers_missing:
                self.log(f'Running producer <{producer.__name__}>.', context)
                producer(**context)
            self.log(f'Loading inputs {list(input)}.', context)
            input_dict = {_.name: _.load(context) for _ in input}
            kwargs = {**input_dict, **context}
            self.log(f'Attempting to run function <{func.__name__}>.', context)
            results = func(**kwargs)
            return results

        return func_wrapped

    def _create_output_wrapper(self, func, output):

        @wraps(func)
        def func_wrapped(**context):
            self.log(f'Checking if outputs of function <{func.__name__}> exist.', context)
            missing = [_ for _ in output if not _.check(context)]
            if missing:
                self.log(f'Missing outputs {missing} of function <{func.__name__}>.', context)
            elif context['force']:
                self.log(f'Force-running function <{func.__name__}>.', context)
            else:
                self.log(f'Skipping function <{func.__name__}>, because all outputs exist.', context)
                return
            results = func(**context)
            if not isinstance(results, tuple):
                results = (results,)
            self.log(f'Saving outputs of function <{func.__name__}>.', context)
            resources = self.outputs[func]
            for resource, result in zip(resources, results):
                resource.save(result, context)
            return results

        return func_wrapped

    def input(self, *input):
        def decorator(func):
            func_wrapped = self._create_input_wrapper(func, input)
            return func_wrapped
        return decorator

    def output(self, *output):
        def decorator(func):
            func_wrapped = self._create_output_wrapper(func, output)
            self.log(f'Registering {list(output)} as output of <{func.__name__}>', verbose=self.verbose_during_setup)
            self.outputs[func] = output
            for resource in output:
                self.producers[resource] = func_wrapped
            self.funcs[func.__name__] = func_wrapped
            return func_wrapped
        return decorator

    def cli(self):
        '''
        Runs the default command line interface of this `Pipeline`.
        '''
        pipeline_cli = PipelineCLI(self)
        pipeline_cli.run()

    def log(self, message, context={'verbose': False}, verbose=False):
        '''
        Logs the supplied message which is currently equivalent to printing (to be improved).
        Additionally, the message is verbosed to the command line if either the `context['verbose']` or the keyword
        argument `verbose` is True. The keyword argument is essential for logging during DAG definition, because at
        this time, there is no context available yet.
        '''
        if verbose or context['verbose']:
            print(message)

    def run(self, task=None, force=False, verbose=False, **context):
        context['task'] = task
        context['force'] = force
        context['verbose'] = verbose
        pretty_context = pprint.pformat(context)
        pretty_indented_context = '\n'.join(['  ' + _ for _ in pretty_context.split('\n')])
        self.log('Running with context:\n' + pretty_indented_context, context)
        if task:
            task = self.funcs[task]
            task(**context)
        else:
            self.log('Auto-running DAG.', context)
            for func in self.funcs.values():
                self.log(f'Attempting function <{func.__name__}>.', context)
                func(**context)


class PipelineCLI(argparse.ArgumentParser):

    def __init__(self, pipeline):
        super().__init__()
        self.pipeline = pipeline
        self.add_argument('-t', '--task', help='run a specific task')
        self.add_argument('-f', '--force', action='store_true',
                          help='force task to run even if its output already exists')
        self.add_argument('-v', '--verbose', action='store_true', help='be verbose about pipeline internals')

    def run(self, context={}):
        args = self.parse_args()
        context = {**vars(args), **context}
        self.pipeline.run(**context)


def _assert_resources_uniqueness(resources, keys):
    for resource, uniqueness_keys in keys.items():
        rows_per_group = resources[resource].groupby(uniqueness_keys).size()
        assert np.all(rows_per_group == 1), \
            f'Resource <{resource}> contains duplicates for key identifers {uniqueness_keys}.'


def assert_input_uniqueness(**keys):
    '''
    Decorator to assert that function inputs have no duplicate entries for a set of unique identifier key columns.
    '''
    def decorator(func):

        @wraps(func)
        def decorated_func(*args, **kwargs):
            _assert_resources_uniqueness(kwargs, keys)
            return func(*args, **kwargs)

        return decorated_func
    return decorator


def assert_output_uniqueness(**keys):
    '''
    Decorator to assert that function outputs have no duplicate entries for a set of unique identifier key columns.
    '''
    def decorator(func):

        @wraps(func)
        def decorated_func(*args, **kwargs):
            results = func(*args, **kwargs)
            _assert_resources_uniqueness(results, keys)
            return results

        return decorated_func
    return decorator


def _assert_resources_completeness(resources, ids):
    for resource in ids:
        df = resources[resource]
        number_of_nas = df.isnull().sum()
        columns_with_nas = number_of_nas[number_of_nas > 0]
        assert len(columns_with_nas) == 0, f'Resource <{resource}> contains NA values: {columns_with_nas.to_dict()}.'


def assert_input_completeness(*input):
    '''
    Decorator to assert that input data has no missing values.
    '''
    def decorator(func):

        @wraps(func)
        def decorated_func(*args, **kwargs):
            _assert_resources_completeness(kwargs, input)
            return func(*args, **kwargs)

        return decorated_func
    return decorator


def assert_output_completeness(*output):
    '''
    Decorator to assert that output data has no missing values.
    '''
    def decorator(func):

        @wraps(func)
        def decorated_func(*args, **kwargs):
            results = func(*args, **kwargs)
            _assert_resources_completeness(results, output)
            return results

        return decorated_func
    return decorator


def _assert_resources_range(resources, ranges):
    for resource_id, column_ranges in ranges.items():
        resource = resources[resource_id]
        for column, column_range in column_ranges.items():
            column_min = resource[column].min()
            column_max = resource[column].max()
            assert column_min >= column_range[0], \
                f'Column <{column}> of resource <{resource_id}> does not meet minimum value requirement. ' + \
                f'Present minimum value: {column_min}. Expected minimum: {column_range[0]}.'
            assert column_max <= column_range[1], \
                f'Column <{column}> of resource <{resource_id}> does not meet maximum value requirement. ' + \
                f'Present maximum value: {column_max}. Expected maximum: {column_range[1]}.'


def assert_input_range(**input):
    '''
    Decorator to assert that input data is in a specific range.
    '''
    def decorator(func):

        @wraps(func)
        def decorated_func(*args, **kwargs):
            _assert_resources_range(kwargs, input)
            return func(*args, **kwargs)

        return decorated_func
    return decorator


def assert_output_range(**output):
    '''
    Decorator to assert that output data is in a specific range.
    '''
    def decorator(func):

        @wraps(func)
        def decorated_func(*args, **kwargs):
            results = func(*args, **kwargs)
            _assert_resources_range(results, output)
            return results

        return decorated_func
    return decorator
