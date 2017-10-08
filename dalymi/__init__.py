import argparse
from functools import wraps
import os.path

import pandas as pd


DEFAULT_CONTEXT = {'execution_date': pd.to_datetime('now'),
                   'force_run': False}


class Pipeline:

    def __init__(self, read_func, write_func, check_input=os.path.isfile):
        self.read_func = read_func
        self.write_func = write_func
        self.check_input = check_input

    def ensure_input(self, dependencies, context):
        for producer in dependencies:
            products = dependencies[producer]
            for product in products:
                product = product.format(**context)
                if not self.check_input(product):
                    print('Missing input', product)
                    producer(**context)
                    break
                elif context['force_run']:
                    print('Forcing', producer)
                    producer(**context)
                    break

    def input(self, **dependencies):
        def in_decorator(func):
            @wraps(func)
            def func_wrapped(**context):
                print('Attempting', func)
                self.ensure_input(dependencies, context)
                print('Running', func)
                return func(**context)
            return func_wrapped
            func_wrapped.__dalymi_pipe_task = True
        return in_decorator

    def run(self, tasks, task=None, context=DEFAULT_CONTEXT):
        if task is not None:
            task(**context)
        else:
            # auto-detect tasks without downstream dependencies and run them
            pass

    def run_cli(self, tasks):
        parser = argparse.ArgumentParser()
        parser.add_argument('-t', '--task')
        parser.add_argument('-e', '--execution-date', type=pd.to_datetime, default=pd.to_datetime('now'))
        parser.add_argument('-f', '--force-run')
        args = parser.parse_args()
        context = DEFAULT_CONTEXT.copy()
        args_dict = vars(args)
        task = args_dict.pop('task')
        context.update(args_dict)
        self.run(tasks, task=task, context=context)
