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
        self.consumers = {}
        self.verbose_during_setup = verbose_during_setup

    def _create_io_wrapper(self, func, input, output):
        name = func.__name__

        @wraps(func)
        def func_wrapped(**context):
            # @TODO: simplify this code by
            # check which inputs are missing
            # if none, skip (return)
            # if some, check which producers need to run
            # run producers
            # load inputs
            # save results
            self.log(f'Checking if all outputs of function <{name}> exists.', context)
            for resource in output:
                fpath = self.resources[resource]
                path = fpath.format(**context)
                if not self.check_resource(path):
                    self.log(f'Missing output <{resource}> of function <{name}> at <{path}>.', context)
                    self.log(f'Attempting to load input for function <{name}>.', context)
                    input_dict = self.load_resources(input, context)
                    self.log(f'Attempting to run function <{name}>.', context)
                    kwargs = {**input_dict, **context}
                    results = func(**kwargs)
                    self.log(f'Saving outputs of function <{name}>.', context)
                    self.save_results(results, context)
                    break
            else:
                self.log(f'Skipping function <{name}>, because all outputs exist.', context)

        return func_wrapped

    def cli(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-t', '--task', help='run a specific task')
        parser.add_argument('-f', '--force', action='store_true',
                            help='force tasks to run even if they already have output')
        parser.add_argument('-v', '--verbose', action='store_true', help='be verbose about pipeline internals')
        args = parser.parse_args()
        context = vars(args)
        self.run(**context)

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
        # run producers if resources do not exist
        for resource in resources:
            fpath = self.resources[resource]
            path = fpath.format(**context)
            if not self.check_resource(path):
                self.log(f'Missing input <{resource}> at <{path}>.', context)
                producer = self.producers[resource]
                self.log(f'Attempting to run function <{producer.__name__}>.', context)
                producer(**context)
        # now load all resources
        resources_dict = {}
        for resource in resources:
            fpath = self.resources[resource]
            path = fpath.format(**context)
            self.log(f'Loading resource <{resource}>.', context)
            resources_dict[resource] = self.load_resource(path)
        return resources_dict

    def register_dag_func(self, func, input, output):
        if input:
            self.log(f'Registering function <{name}> as comsumer of {input}.', verbose=self.verbose_during_setup)
            self.consumers[func.__name__] = func
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
            task = self.consumers[task]
            task(**context)
        else:
            self.log('Auto-running DAG.', context)
            for name, consumer in self.consumers.items():
                self.log(f'Attemping function <{name}>.', context)
                consumer(**context)

    def save_results(self, results, context):
        for resource, result in results.items():
            fpath = self.resources[resource]
            path = fpath.format(**context)
            self.log(f'Saving resource <{resource}> at <{path}>.', context)
            self.save_resource(result, path)
