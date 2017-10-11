import argparse
from functools import wraps
import os.path

import pandas as pd


DEFAULT_CONTEXT = {'execution_date': pd.to_datetime('now'),
                   'force_run': False}


class Pipeline:

    def __init__(self, load_target, save_target, check_target=os.path.isfile):
        self.load_target = load_target
        self.save_target = save_target
        self.check_target = check_target

    def ensure_targets(self, dependencies, context):
        for dep in dependencies:
            if not self.check_targets(dep.__dalymi_outputs, context):
                print('Missing output of dependency', dep)
                print('Attempting', dep)
                dep(**context)
            elif context['force_run']:
                print('Forcing', dep)
                dep(**context)

    def load_inputs(self, dependencies, context):
        inputs = {}
        for dep in dependencies:
            results = self.load_targets(dep.__dalymi_outputs, context)
            inputs.update(results)
        return inputs

    def check_targets(self, targets, context):
        for fpath in targets.values():
            path = fpath.format(**context)
            if not self.check_target(path):
                return False
        return True

    def load_targets(self, targets, context):
        results = {}
        for target, fpath in targets.items():
            path = fpath.format(**context)
            results[target] = self.load_target(path)
        return results

    def save_targets(self, targets, results, context):
        for target, fpath in targets.items():
            path = fpath.format(**context)
            result = results[target]
            self.save_target(result, path)

    def ensure_dependencies(self, *dependencies):
        '''
        A decorator that ensures that dependencies of the decorated function have produced their output, specified by their save_output decorator.
        If any output of a dependency is not found by, then the dependency is run before running the decorated function.
        Outputs of dependencies are loaded and provided to the task as keyword arguments in addition to the context.
        '''
        def ensure_dependencies_decorator(func):
            @wraps(func)
            def func_wrapped(**context):
                print('Ensuring dependencies of', func)
                self.ensure_targets(dependencies, context)
                print('All dependencies satisfied for', func)
                inputs = self.load_inputs(dependencies, context)
                kwargs = {**inputs, **context}
                print('Attempting', func)
                results = func(**kwargs)
                return results
            # add a function attribute, so that this function can be identified as a dalymi task.
            # this is useful for auto-detecting the DAG if non-dalymi tasks are present in the tasks dict, for example when locals() was supplied.
            func_wrapped.__dalymi_dependencies = True
            return func_wrapped
        return ensure_dependencies_decorator

    def save_output(self, *args, **kwargs):
        '''
        A decorator that does two things:
            (a) saves objects returned by the decorated function to the provided targets. The path is formatted with the context  beforehand.
            (b) registers **outputs as a function attribute, so that downstream tasks can check if the decorated function is fulfilled as a dependency.
        '''
        def save_output_decorator(func):
            @wraps(func)
            def func_wrapped(**context):
                # check if outputs already exist
                if self.check_targets(outputs, context):
                    print('Loading outputs from target for', func)
                    results = self.load_targets(outputs, context)
                else:
                    print('Missing targets for', func)
                    print('Attempting', func)
                    results = func(**context)
                    print('Writing targets for', func)
                    self.save_targets(outputs, results, context)
                return results
            # register output dict for downstream tasks
            func_wrapped.__dalymi_outputs = outputs
            return func_wrapped
        return save_output_decorator

    def run(self, task=None, context=DEFAULT_CONTEXT):
        if task is not None:
            task(**context)
        else:
            # auto-detect tasks without downstream dependencies and run them
            pass

    def run(self, tasks):
        parser = argparse.ArgumentParser()
        parser.add_argument('-t', '--task')
        parser.add_argument('-e', '--execution-date', type=pd.to_datetime, default=pd.to_datetime('now'))
        parser.add_argument('-f', '--force-run', action='store_true')
        args = parser.parse_args()
        args_dict = vars(args)
        task_str = args_dict.pop('task')
        if task_str:
            task = tasks[task_str]
        else:
            # auto-detect dag
            pass
        context = DEFAULT_CONTEXT.copy()
        context.update(args_dict)
        task(**context)
