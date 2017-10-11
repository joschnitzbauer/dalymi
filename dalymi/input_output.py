from functools import wraps

import pandas as pd


class IO:

    def __init__(self, load_resource=pd.read_csv, save_resource=lambda df: df.to_csv):
        self.load_resource = load_resource
        self.save_resource = save_resource
        self.resources = {}

    def _load_resources(self, resources, context):
        resources_dict = {}
        for resource in resources:
            fpath = self.resources[resource]
            path = fpath.format(**context)
            resources_dict[resource] = self.load_resource(path)
        return resources_dict

    def _save_resources(self, resources, results, context):
        for resource, fpath in resources.items():
            path = fpath.format(**context)
            result = results[resource]
            self.save_resource(result, path)

    def load_input(self, *inputs):
        def load_output_decorator(func):
            @wraps(func)
            def func_wrapped(**context):
                print('Loading input of', func)
                inputs_dict = self._load_resources(inputs, context)
                kwargs = {**inputs_dict, **context}
                return func(**kwargs)
            return func_wrapped
        return load_output_decorator

    def save_output(self, **outputs):
        def save_output_decorator(func):
            @wraps(func)
            def func_wrapped(**context):
                results = func(**context)
                print('Saving output of', func)
                self._save_resources(outputs, results, context)
                return results
            return func_wrapped
            # register the output so that other functions can find it via `load_input`
            self.resources.update(outputs)
        return save_output_decorator