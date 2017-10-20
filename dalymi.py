import argparse
from functools import wraps
import os.path
import pprint

import pandas as pd
import numpy as np


class Pipeline:

    def __init__(self, load_resource=pd.read_csv, save_resource=lambda df, path: df.to_csv(path),
                 check_resource=os.path.isfile, verbose_during_setup=False):
        self.load_resource = load_resource
        self.save_resource = save_resource
        self.check_resource = check_resource
        self.resources = {}
        self.producers = {}
        self.funcs = {}
        self.verbose_during_setup = verbose_during_setup

    def _create_io_wrapper(self, func, input, output):
        '''
        Creates a wrapped function of `func` which can be returned by the `io` decorator.
        This is the core logic of DAG functions.

        Args:
            input (list): resource IDs
            output (dict): a dictionary with resource IDs as keys and target templates as values.

        Returns:
            The wrapped version of `func`.
        '''
        name = func.__name__

        @wraps(func)
        def func_wrapped(**context):
            self.log(f'Checking if outputs of function <{name}> exist.', context)
            existing, missing = self.check_output(output, context)
            if missing:
                self.log(f'Missing outputs {missing} of function <{name}>.', context)
            elif context['force']:
                self.log(f'Force-running function <{name}>.', context)
            else:
                self.log(f'Skipping function <{name}>, because all outputs exist.', context)
                return
            existing, missing = self.check_input(input, context)
            producers = self.get_producers(missing)
            for producer in producers:
                self.log(f'Running producer {producer.__name__}.', context)
                producer(**context)
            self.log(f'Loading inputs {input}.', context)
            input_dict = self.load_resources(input, context)
            kwargs = {**input_dict, **context}
            self.log(f'Attempting to run function <{name}>.', context)
            results = func(**kwargs)
            self.log(f'Saving outputs of function <{name}>.', context)
            self.save_results(results, context)

        return func_wrapped

    def check_input(self, input, context):
        '''
        Checks whether the specified input resources are available.

        Args:
            input (list): resource IDs
            context (dict): the context

        Returns:
            existing (list): resource IDs with exisiting target
            missing (list): resource IDs with missing target
        '''
        existing = []
        missing = []
        for resource in input:
            fpath = self.resources[resource]
            path = fpath.format(**context)
            if self.check_resource(path):
                existing.append(resource)
            else:
                missing.append(resource)
        return existing, missing

    def check_output(self, output, context):
        '''
        Checks whether the specified output resources are available.

        Args:
            output (dict): a dictionary with resource IDs as keys and target templates as values.
            context (dict): the context

        Returns:
            existing (list): resource IDs with exisiting target
            missing (list): resource IDs with missing target
        '''
        existing = []
        missing = []
        for resource, fpath in output.items():
            path = fpath.format(**context)
            if self.check_resource(path):
                existing.append(resource)
            else:
                missing.append(resource)
        return existing, missing

    def cli(self):
        '''
        Runs the default command line interface of this `Pipeline`.
        '''
        pipeline_cli = PipelineCLI(self)
        pipeline_cli.run()

    def get_producers(self, resources):
        '''
        Returns the producers of the specified resources.

        Args:
            resources (list): resource IDs

        Returns:
            producers (list): function names of producers
        '''
        producers = []
        for resource in resources:
            producer = self.producers[resource]
            producers.append(producer)
        return producers

    def io(self, input=[], output={}):
        '''
        A decorator to specify which input and output resources the decorated function produces.
        Registers the function and output resources in the `Pipeline`.

        Args:
            input (list): a list of consumed resource IDs which are loaded and supplied to the decorated function as
                          keyword arguments.
            output (dict): a dictionary to specify which under which resource ID the function outputs should be
                           registered (keys), and what their resource template is (values)

        Returns:
            The decorated function.
        '''
        def decorator(func):
            func_wrapped = self._create_io_wrapper(func, input, output)
            self.register_dag_func(func_wrapped, input, output)
            return func_wrapped
        return decorator

    def log(self, message, context={'verbose': False}, verbose=False):
        '''
        Logs the supplied message which is currently equivalent to printing (to be improved).
        Additionally, the message is verbosed to the command line if either the `context['verbose']` or the keyword
        argument `verbose` is True. The keyword argument is essential for logging during DAG definition, because at
        this time, there is no context available yet.
        '''
        if verbose or context['verbose']:
            pprint.pprint(message)

    def load_resources(self, resources, context):
        '''
        Loads resources into memory.

        Args:
            resources (list): a list of resource IDs
            context (dict): the context

        Returns:
            resources (dict): a dictionary of resources with IDs as keys and data frames as values
        '''
        resources_dict = {}
        for resource in resources:
            fpath = self.resources[resource]
            path = fpath.format(**context)
            self.log(f'Loading resource <{resource}>.', context)
            resources_dict[resource] = self.load_resource(path)
        return resources_dict

    def register_dag_func(self, func, input, output):
        '''
        Stores relevant information for the DAG of the given function.

        Args:
            func (callable): the function to be registered
            input (list): resource IDs of function input
            output (list): resource IDs of function output
        '''
        name = func.__name__
        self.log(f'Registering function <{name}> as DAG function.', verbose=self.verbose_during_setup)
        self.funcs[name] = func
        if output:
            self.log(f'Registerung function <{name}> as producer of {list(output.keys())}.', verbose=self.verbose_during_setup)
            for resource in output:
                self.producers[resource] = func
            self.log(f'Registering resources {output} of function <{name}>.', verbose=self.verbose_during_setup)
            self.resources.update(output)

    def run(self, task=None, force=False, verbose=False, **context):
        context['task'] = task
        context['force'] = force
        context['verbose'] = verbose
        self.log('Running with context:', context)
        self.log(f'{context}', context)
        if task:
            task = self.funcs[task]
            task(**context)
        else:
            self.log('Auto-running DAG.', context)
            for name, func in self.funcs.items():
                self.log(f'Attemping function <{name}>.', context)
                func(**context)

    def save_results(self, results, context):
        for resource, result in results.items():
            fpath = self.resources[resource]
            path = fpath.format(**context)
            self.log(f'Saving resource <{resource}> at <{path}>.', context)
            self.save_resource(result, path)


class PipelineCLI(argparse.ArgumentParser):

    def __init__(self, pipeline):
        super().__init__()
        self.pipeline = pipeline
        self.add_argument('-t', '--task', help='run a specific task')
        self.add_argument('-f', '--force', action='store_true',
                          help='force tasks to run even if they already have output')
        self.add_argument('-v', '--verbose', action='store_true', help='be verbose about pipeline internals')

    def run(self, context={}):
        args = self.parse_args()
        context = {**vars(args), **context}
        self.pipeline.run(**context)


def _assert_resources_columns(resources, columns):
    for resource, resource_columns in columns.items():
        assert set(resources[resource].columns) == set(resource_columns), \
            f'Columns of resource <{resource}> do not match expected. ' \
            + f'Present: {set(resources[resource].columns)}. Expected: {set(resource_columns)}.'


def assert_input_columns(**columns):
    '''
    Decorator to assert that the column names of input dataframes are exactly what is expected.
    Expected column names are supplied as lists for each input resource ID as keyword arguments.
    '''
    def decorator(func):

        @wraps(func)
        def decorated_func(*args, **kwargs):
            _assert_resources_columns(kwargs, columns)
            return func(*args, **kwargs)

        return decorated_func
    return decorator


def assert_output_columns(**columns):
    '''
    Decorator to assert that the column names of output dataframes are exactly what is expected.
    Expected column names are supplied as lists for each output resource ID as keyword arguments.
    '''
    def decorator(func):

        @wraps(func)
        def decorated_func(*args, **kwargs):
            results = func(*args, **kwargs)
            _assert_resources_columns(results, columns)
            return results

        return decorated_func
    return decorator


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
        assert columns_with_nas == 0, f'Resource <{resource}> contains NA values: {columns_with_nas.to_dict()}.'


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
