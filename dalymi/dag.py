import argparse
from functools import wraps


class DAG:

    def __init__(self):
        self.tasks_with_dependencies = {}
        self.finished_tasks = []
        self.force_run = False
        self.verbose = False

    def cli(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-t', '--task')
        parser.add_argument('-f', '--force-run', action='store_true')
        parser.add_argument('-v', '--verbose', action='store_true')
        args = parser.parse_args()
        self.run(**vars(args))

    def ensure_dependencies(self, *dependencies):
        '''
        A decorator ensuring that dependencies of the decorated function have run.
        Outputs of dependencies are loaded and provided to the task as keyword arguments in addition to `context`.
        '''
        def ensure_dependencies_decorator(func):
            @wraps(func)
            def func_wrapped(*args, **kwargs):
                self.log('Ensuring dependencies of', func)
                self.ensure_tasks(dependencies)
                self.log('Attempting', func)
                results = func(*args, **kwargs)
                return results
            self.tasks_with_dependencies[func_wrapped.__name__] = func_wrapped
            return func_wrapped
        return ensure_dependencies_decorator

    def ensure_task(self, task):
        '''
        Ensures that the given task has run.
        '''
        if task in self.finished_tasks:
            if self.force_run:
                self.log('Forcing', task)
                self.run_task(task)
            else:
                self.log('Skipping', task)
        else:
            self.log('Attempting', task)
            self.run_task(task)

    def ensure_tasks(self, tasks):
        '''
        Ensures that the given tasks have run.
        '''
        for task in tasks:
            self.ensure_task(task)

    def log(self, *message):
        if self.verbose:
            print(*message)

    def run(self, task=None, force_run=False, verbose=False):
        self.verbose = verbose
        if task:
            task = self.tasks_with_dependencies[task]
            self.ensure_task(task)
        else:
            self.ensure_tasks(self.tasks_with_dependencies.values())

    def run_task(self, task):
        '''
        Runs the task and registers it as finished when done.
        '''
        task()
        self.finished_tasks.append(task)
