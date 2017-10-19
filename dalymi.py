import argparse
from functools import wraps
import os.path

import pandas as pd


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
        pipeline_cli = PipelineCLI(self)
        pipeline_cli.run()

    def get_producers(self, resources):
        producers = []
        for resource in resources:
            producer = self.producers[resource]
            producers.append(producer)
        return producers

    def io(self, input=[], output={}):
        def decorator(func):
            func_wrapped = self._create_io_wrapper(func, input, output)
            self.register_dag_func(func_wrapped, input, output)
            return func_wrapped
        return decorator

    def log(self, message, context={'verbose': False}, verbose=False):
        if verbose or context['verbose']:
            print(message)

    def load_resources(self, resources, context):
        resources_dict = {}
        for resource in resources:
            fpath = self.resources[resource]
            path = fpath.format(**context)
            self.log(f'Loading resource <{resource}>.', context)
            resources_dict[resource] = self.load_resource(path)
        return resources_dict

    def register_dag_func(self, func, input, output):
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

    def run(self):
        args = self.parse_args()
        context = vars(args)
        self.pipeline.run(**context)
