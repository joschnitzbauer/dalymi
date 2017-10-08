import argparse
from functools import wraps
import os.path


class Pipe:

    def __init__(self, read_func, write_func):
        self.read_func = read_func
        self.write_func = write_func
        self.context = {}

    def ensure_input(self, **producers):


    def input(self, **producers):
        def in_decorator(func):
            @wraps(func)
            def func_wrapped(*args, **kwargs):
                self.ensure_input(**producers)
                return func(**args, **kwargs)
            return func_wrapped
            func_wrapped.__dalymi_pipe_func = True
        return in_decorator

    def run(self):
        pass
