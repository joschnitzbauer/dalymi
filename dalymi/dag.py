import argparse
from functools import wraps

import pandas as pd


DEFAULT_CONTEXT = {'execution_date': pd.to_datetime('now'),
                   'force_run': False}


class DAG:

    def __init__(self):
        self.finished_tasks = []

    def ensure_dependencies(self, *dependencies):
        '''
        A decorator ensuring that dependencies of the decorated function have run.
        Outputs of dependencies are loaded and provided to the task as keyword arguments in addition to `context`.
        '''
        def ensure_dependencies_decorator(func):
            @wraps(func)
            def func_wrapped(**context):
                print('Ensuring dependencies of', func)
                self.ensure_tasks(dependencies)
                print('Attempting', func)
                results = func(**context)
                return results
            return func_wrapped
        return ensure_dependencies_decorator

    def ensure_task(self, task, context):
        '''
        Ensures that the given task has run.
        '''
        if task in self.finished_tasks:
            if context['force_run']:
                print('Forcing', task)
                self.run_task(task, context)
            else:
                print('Skipping', task)
        else:
            print('Attempting', task)
            self.run_task(task, context)

    def ensure_tasks(self, tasks, context):
        '''
        Ensures that the given tasks have run.
        '''
        for task in tasks:
            self.ensure_task(task, context)

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

    def run_task(self, task, context):
        '''
        Runs the task and registers it as finished when done.
        '''
        task(**context)
        self.finished_tasks.append(task)
