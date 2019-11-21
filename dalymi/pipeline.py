import argparse
from functools import wraps
import itertools
import logging
import pprint


class Pipeline:
    '''
    The main API to generate dalymi pipelines.
    '''

    def __init__(self):
        self.outputs = {}    # keys: funcs, values: list of output resources
        self.producers = {}  # keys: resources, values: funcs_wrapped
        self.funcs = {}      # keys: func names, values: funcs_wrapped
        self.consumers = []  # list of (resource name, func name)
        self.original_funcs = {}  # keys: funcs_wrapped, values: funcs

    def _create_input_wrapper(self, func, input):

        @wraps(func)
        def func_wrapped(**context):
            missing = [_ for _ in input if not _._check(context)]
            producers_missing = [self.producers[_] for _ in missing]
            for producer in producers_missing:
                self.log('Running producer <{}>.'.format(producer.__name__))
                producer(**context)
            self.log('Loading inputs {}.'.format([_.name for _ in input]))
            input_dict = {_.name: _._load(context) for _ in input}
            kwargs = {**input_dict, **context}
            self.log('Attempting to run function <{}>.'.format(func.__name__))
            results = func(**kwargs)
            return results

        return func_wrapped

    def _create_output_wrapper(self, func, output):

        @wraps(func)
        def func_wrapped(**context):
            self.log('Checking if outputs of function <{}> exist.'.format(func.__name__))
            missing = [_ for _ in output if not _._check(context)]
            if missing:
                self.log('Missing outputs {} of function <{}>.'.format([_.name for _ in missing], func.__name__))
            else:
                self.log('Skipping function <{}>, because all outputs exist.'.format(func.__name__))
                return
            results = func(**context)
            if not isinstance(results, tuple):
                results = (results,)
            self.log('Saving outputs of function <{}>.'.format(func.__name__))
            resources = self.outputs[func]
            for resource, result in zip(resources, results):
                resource._save(result, context)
            return results

        return func_wrapped

    def input(self, *input):
        '''
        A decorator to specify input resources for the decorated task.

        !!! warning
            A potential `output` decorator **must** wrap an `input` decorator to ensure correct pipeline functionality.

        # Arguments
        *input: a list of resource objects
        '''
        def decorator(func):
            func_wrapped = self._create_input_wrapper(func, input)
            self.log('Registering <{}> as a consumer function.'.format(func.__name__))
            self.consumers.extend([(_.name, func.__name__) for _ in input])
            # This will be overwritten by an output decorator, because the output decorator
            # has to wrap the input decorator:
            self.funcs[func.__name__] = func_wrapped
            # Same here:
            self.original_funcs[func_wrapped] = func
            return func_wrapped
        return decorator

    def output(self, *output):
        '''
        A decorator to specify output resources for the decorated task.

        !!! warning
            The `output` decorator **must** wrap a potential `input` decorator to ensure correct pipeline
            functionality.

        # Arguments
        *output: a list of resource objects
        '''
        def decorator(func):
            func_wrapped = self._create_output_wrapper(func, output)
            self.log('Registering {} as output of <{}>.'.format([_.name for _ in output], func.__name__))
            self.outputs[func] = output
            for resource in output:
                self.producers[resource] = func_wrapped
            self.log('Registering <{}> as producer function.'.format(func.__name__))
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

    def dot(self, T='pdf'):
        dot = 'digraph pipeline {\n'
        for func in self.funcs:
            dot += '\t{} [fontsize=13]\n'.format(func)
        for resource, func in self.producers.items():
            table = '<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0">'
            table += '<TR><TD bgcolor="grey">{}</TD></TR>'.format(resource.name)
            if hasattr(resource, 'columns') and resource.columns is not None:
                for column in resource.columns:
                    table += '<TR><TD>{}</TD></TR>'.format(column)
            table += '</TABLE>>'
            dot += '\t{} [label={} fontsize=12 height=0 margin=0 shape=none width=0]'.format(resource.name, table)
            # the edge:
            dot += '\t{} -> {}\n'.format(func.__name__, resource.name)
        # edges for consumers:
        for resource_name, func_name in self.consumers:
            dot += '\t{} -> {}\n'.format(resource_name, func_name)
        dot += '}\n'
        with open('pipeline.dot', 'w') as f:
            f.write(dot)

    def log(self, message):
        '''
        Logs the supplied message to a Python logger named `__name__` on log level `INFO`.
        '''
        logger = logging.getLogger(__name__)
        logger.info(message)

    def ls(self):
        tasks = list(self.funcs.keys())
        msg = 'Tasks in pipeline:\n'
        for task in tasks:
            msg += '\t{}\n'.format(task)
        print(msg, end='')

    def run(self, task=None, **context):
        context['task'] = task
        pretty_context = pprint.pformat(context)
        pretty_indented_context = '\n'.join(['  ' + _ for _ in pretty_context.split('\n')])
        self.log('Running with context:\n' + pretty_indented_context)
        if task:
            func = self.funcs[task]
            func(**context)
        else:
            self.log('Auto-running DAG.')
            for func in self.funcs.values():
                self.log('Attempting function <{}>.'.format(func.__name__))
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
                self.log('Deleting <{}> at \'{}\'.'.format(output.name, loc))
                output._delete(context)

    def undo(self, task=None, downstream=False, **context):
        context['task'] = task
        context['downstream'] = downstream
        pretty_context = pprint.pformat(context)
        pretty_indented_context = '\n'.join(['  ' + _ for _ in pretty_context.split('\n')])
        self.log('Undoing with context:\n' + pretty_indented_context)
        if task and downstream:
            tasks_to_undo = self.get_downstream_tasks(task)
            tasks_to_undo.add(task)
        elif task and not downstream:
            tasks_to_undo = [task]
        else:
            tasks_to_undo = self.funcs.keys()
        self.log('Undoing tasks {}.'.format(list(tasks_to_undo)))
        self.delete_output(tasks_to_undo, context)


class PipelineCLI():
    '''
    A class representing the command line interface of a `Pipeline`.

    # Arguments
    pipeline (dalymi.pipeline.Pipeline): the pipeline object to create a CLI for.

    # Attributes
    run_parser (argparse.ArgumentParser): handles the `run` sub-command
    dot_parser (argparse.ArgumentParser): handles the `dot` sub-command
    ls_parser (argparse.ArgumentParser): handles the `ls` sub-command
    '''

    def __init__(self, pipeline):
        self.pipeline = pipeline

        self.parser = argparse.ArgumentParser()
        self.subparsers = self.parser.add_subparsers(dest='command')

        self.run_parser = self.subparsers.add_parser('run', help='run the pipeline')
        self.run_parser.add_argument('-t', '--task', help='run/undo a specific task')

        self.dot_parser = self.subparsers.add_parser('dot', help='create a graphviz dot file of the DAG')

        self.ls_parser = self.subparsers.add_parser('ls', help='list pipeline tasks')

    def run(self, external_context={}):
        '''
        Parses arguments and runs the provided command.
        '''
        undo_parser = self.subparsers.add_parser('undo', parents=[self.run_parser], add_help=False,
                                                 description='undo tasks')
        undo_parser.add_argument('-d', '--downstream', action='store_true', help='undo downstream tasks')
        args = self.parser.parse_args()
        context = {**external_context, **vars(args)}
        if args.command == 'run':
            self.pipeline.run(**context)
        elif args.command == 'undo':
            self.pipeline.undo(**context)
        elif args.command == 'dot':
            self.pipeline.dot()
        elif args.command == 'ls':
            self.pipeline.ls()
        else:
            self.parser.print_help()
