import argparse
from functools import wraps
import itertools
import os.path
import pprint
import pickle

import pandas as pd
import numpy as np


class Resource:

    def __init__(self, name=None, loc=None, assertions=[]):
        self.name = name
        self.loc = loc
        self.assertions = assertions

    def assert_integrity(self, data):
        for assertion in self.assertions:
            assertion(data)

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


class LocalFileResource(Resource):

    def check(self, path):
        return os.path.isfile(path)

    def delete(self, path):
        return os.remove(path)


class PandasDFResource(LocalFileResource):

    def __init__(self, name=None, loc=None, columns=None, custom_assertions=[]):
        assertions = [self.assert_columns] + custom_assertions
        super().__init__(name, loc, assertions=assertions)
        self.columns = columns

    def assert_columns(self, df):
        if self.columns is not None:
            assert set(df.columns) == set(self.columns), \
                f'Columns of resource <{self.name}> do not match expected. ' \
                + f'Present: {set(df.columns)}. Expected: {set(self.columns)}.'

    def load(self, path):
        return pd.read_csv(path)

    def save(self, path, data):
        return data.to_csv(path, index=False)


class PickleResource(LocalFileResource):

    def __init__(self, name=None, loc=None, custom_assertions=[]):
        super().__init__(name, loc, assertions=custom_assertions)

    def load(self, path):
        with open(path, 'rb') as f:
            return pickle.load(f)

    def save(self, path, data):
        with open(path, 'wb') as f:
            pickle.dump(data, f)


class Pipeline:

    def __init__(self, verbose_during_setup=False):
        self.outputs = {}    # keys: funcs, values: list of output resources
        self.producers = {}  # keys: resources, values: funcs_wrapped
        self.funcs = {}      # keys: func names, values: funcs_wrapped
        self.consumers = []  # list of (resource name, func name)
        self.original_funcs = {}  # keys: funcs_wrapped, values: funcs
        self.verbose_during_setup = verbose_during_setup

    def _create_input_wrapper(self, func, input):

        @wraps(func)
        def func_wrapped(**context):
            missing = [_ for _ in input if not _._check(context)]
            producers_missing = [self.producers[_] for _ in missing]
            for producer in producers_missing:
                self.log(f'Running producer <{producer.__name__}>.', context)
                producer(**context)
            self.log(f'Loading inputs {[_.name for _ in input]}.', context)
            input_dict = {_.name: _._load(context) for _ in input}
            kwargs = {**input_dict, **context}
            self.log(f'Attempting to run function <{func.__name__}>.', context)
            results = func(**kwargs)
            return results

        return func_wrapped

    def _create_output_wrapper(self, func, output):

        @wraps(func)
        def func_wrapped(**context):
            self.log(f'Checking if outputs of function <{func.__name__}> exist.', context)
            missing = [_ for _ in output if not _._check(context)]
            if missing:
                self.log(f'Missing outputs {[_.name for _ in missing]} of function <{func.__name__}>.', context)
            else:
                self.log(f'Skipping function <{func.__name__}>, because all outputs exist.', context)
                return
            results = func(**context)
            if not isinstance(results, tuple):
                results = (results,)
            self.log(f'Saving outputs of function <{func.__name__}>.', context)
            resources = self.outputs[func]
            for resource, result in zip(resources, results):
                resource._save(result, context)
            return results

        return func_wrapped

    def input(self, *input):
        def decorator(func):
            func_wrapped = self._create_input_wrapper(func, input)
            self.log(f'Registering <{func.__name__}> as a consumer function.', verbose=self.verbose_during_setup)
            self.consumers.extend([(_.name, func.__name__) for _ in input])
            # just in case we don't have ouput. If we do, this will be overwritten, because the output decorator
            # has to wrap the input decorator:
            self.funcs[func.__name__] = func_wrapped
            self.original_funcs[func_wrapped] = func
            return func_wrapped
        return decorator

    def output(self, *output):
        def decorator(func):
            func_wrapped = self._create_output_wrapper(func, output)
            self.log(f'Registering {[_.name for _ in output]} as output of <{func.__name__}>', verbose=self.verbose_during_setup)
            self.outputs[func] = output
            for resource in output:
                self.producers[resource] = func_wrapped
            self.log(f'Registering <{func.__name__}> as producer function.', verbose=self.verbose_during_setup)
            self.funcs[func.__name__] = func_wrapped
            self.original_funcs[func_wrapped] = func
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

    def plot(self, **kwargs):
        from graphviz import Digraph
        graph = Digraph('pipeline')
        for func in self.funcs:
            graph.node(func, fontname='"Lucida Console", Monaco, Consolas, monospace bold', fontsize='11')
        for resource, func in self.producers.items():
            table = '<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0">'
            table += f'<TR><TD><B>{resource.name}</B></TD></TR>'
            if hasattr(resource, 'columns'):
                for column in resource.columns:
                    table += f'<TR><TD>{column}</TD></TR>'
            table += '</TABLE>>'
            graph.node(resource.name, shape='none', label=table, width='0', height='0', margin='0',
                       fontname='"Lucida Console", Monaco, Consolas, monospace', fontsize='11')
            graph.edge(func.__name__, resource.name)
        graph.edges(self.consumers)
        graph.render(**kwargs)

    def run(self, task=None, execution_date=pd.Timestamp('today').date(), verbose=False, **context):
        context['task'] = task
        context['execution_date'] = execution_date
        context['verbose'] = verbose
        pretty_context = pprint.pformat(context)
        pretty_indented_context = '\n'.join(['  ' + _ for _ in pretty_context.split('\n')])
        self.log('Running with context:\n' + pretty_indented_context, context)
        if task:
            func = self.funcs[task]
            func(**context)
        else:
            self.log('Auto-running DAG.', context)
            for func in self.funcs.values():
                self.log(f'Attempting function <{func.__name__}>.', context)
                func(**context)

    def get_downstream_tasks(self, task):
        func = self.funcs[task]
        original_func = self.original_funcs[func]
        if original_func in self.outputs:
            func_outputs = self.outputs[original_func]
        else:
            func_outputs = []
        consumers = set()
        for output in func_outputs:
            output_consumers = [fn for rn, fn in self.consumers if rn == output.name]
            consumers.update(output_consumers)
            for consumer in output_consumers:
                consumer_consumers = self.get_downstream_tasks(consumer)
                consumers.update(consumer_consumers)
        return consumers

    def delete_output(self, tasks, context):
        funcs = [self.funcs[_] for _ in tasks]
        original_funcs = [self.original_funcs[_] for _ in funcs]
        funcs_outputs = [self.outputs[_] for _ in original_funcs if _ in self.outputs]
        outputs = set(itertools.chain(*funcs_outputs))
        for output in outputs:
            if output._check(context):
                loc = output.loc.format(**context)
                self.log(f'Deleting <{output.name}> at \'{loc}\'.', context)
                output._delete(context)

    def undo(self, task=None, execution_date=pd.Timestamp('today').date(), downstream=False, verbose=False, **context):
        context['task'] = task
        context['execution_date'] = execution_date
        context['downstream'] = downstream
        context['verbose'] = verbose
        pretty_context = pprint.pformat(context)
        pretty_indented_context = '\n'.join(['  ' + _ for _ in pretty_context.split('\n')])
        self.log('Undoing with context:\n' + pretty_indented_context, context)
        if task and downstream:
            tasks_to_undo = self.get_downstream_tasks(task)
            tasks_to_undo.add(task)
        elif task and not downstream:
            tasks_to_undo = [task]
        else:
            tasks_to_undo = self.funcs.keys()
        self.log(f'Undoing tasks {list(tasks_to_undo)}.', context)
        self.delete_output(tasks_to_undo, context)


class PipelineCLI():

    def __init__(self, pipeline):
        self.pipeline = pipeline

        self.parser = argparse.ArgumentParser()
        self.subparsers = self.parser.add_subparsers(dest='command')

        self.run_parser = self.subparsers.add_parser('run', help='run the pipeline')
        self.run_parser.add_argument('-t', '--task', help='run/undo a specific task')
        self.run_parser.add_argument('-e', '--execution-date', default=pd.Timestamp('today').date(),
                                     type=lambda x: pd.to_datetime(x).date(), help='the date of execution')
        self.run_parser.add_argument('-v', '--verbose', action='store_true', help='be verbose about pipeline internals')

        self.plot_parser = self.subparsers.add_parser('plot', help='plot the DAG')

    def run(self, context={}):
        undo_parser = self.subparsers.add_parser('undo', parents=[self.run_parser], add_help=False, description='undo tasks')
        undo_parser.add_argument('-d', '--downstream', action='store_true', help='undo downstream tasks')
        args = self.parser.parse_args()
        context = {**vars(args), **context}
        if args.command == 'run':
            self.pipeline.run(**context)
        elif args.command == 'undo':
            self.pipeline.undo(**context)
        elif args.command == 'plot':
            self.pipeline.plot()
        else:
            self.parser.print_help()


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
