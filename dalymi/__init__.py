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

    def io(self, input=[], output={}):
        def decorator(func):
            name = func.__name__

            @wraps(func)
            def func_wrapped(**context):
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
            if input:
                self.log(f'Registering function <{name}> as comsumer of {input}.', verbose=self.verbose_during_setup)
                self.consumers[func_wrapped.__name__] = func_wrapped
            if output:
                self.log(f'Registerung function <{name}> as producer of {list(output.keys())}.',
                         verbose=self.verbose_during_setup)
                for resource in output:
                    self.producers[resource] = func_wrapped
                self.log(f'Registering resources {output} of function <{name}>.', verbose=self.verbose_during_setup)
                self.resources.update(output)
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

    def run(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-t', '--task', help='run a specific task')
        parser.add_argument('-f', '--force', action='store_true',
                            help='force tasks to run even if they already have output')
        parser.add_argument('-v', '--verbose', action='store_true', help='be verbose about pipeline internals')
        parser.add_argument('-e', '--execution-date', default=pd.to_datetime('now'), type=pd.to_datetime,
                            help='a superficial date for which the pipeline should run')
        args = parser.parse_args()
        context = vars(args)
        context['ds'] = context['execution_date'].date().isoformat()
        context['dts'] = context['execution_date'].to_pydatetime().isoformat()
        self.log('Running with context:', context)
        self.log(f'{context}', context)
        if args.task:
            task = self.consumers[args.task]
            task(**context)
        else:
            self.log('Running all consumers.', context)
            for name, consumer in self.consumers.items():
                self.log(f'Attemping function <{name}>.', context)
                consumer(**context)

    def save_results(self, results, context):
        for resource, result in results.items():
            fpath = self.resources[resource]
            path = fpath.format(**context)
            self.log(f'Saving resource <{resource}> at <{path}>.', context)
            self.save_resource(result, path)
